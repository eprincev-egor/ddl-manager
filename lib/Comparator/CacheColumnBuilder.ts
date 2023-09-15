import { Column } from "../database/schema/Column";
import { Comment } from "../database/schema/Comment";
import {
    Select,
    SelectColumn,
    FuncCall,
    Expression,
    ColumnReference,
    UnknownExpressionElement
} from "../ast";
import { Migration } from "../Migrator/Migration";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { CacheColumnGraph } from "./graph/CacheColumnGraph";
import { CacheColumn } from "./graph/CacheColumn";

export interface CacheColumnBuilderParams {
    driver: IDatabaseDriver;
    migration: Migration;
    database: Database;
    graph: CacheColumnGraph;
}

export class CacheColumnBuilder {

    private driver: IDatabaseDriver;
    private migration: Migration;
    private database: Database;
    private graph: CacheColumnGraph;

    constructor(params: CacheColumnBuilderParams) {
        this.driver = params.driver;
        this.migration = params.migration;
        this.database = params.database;
        this.graph = params.graph;
    }

    async build(cacheColumn: CacheColumn) {
        const columnToCreate = new Column(
            cacheColumn.for.table,
            cacheColumn.name,
            await this.getColumnType(cacheColumn),
            this.getColumnDefault(cacheColumn.select),
            Comment.fromFs({
                objectType: "column",
                cacheSignature: cacheColumn.cache.signature,
                cacheSelect: cacheColumn.select.toString()
            })
        );
        return columnToCreate;
    }

    private async getColumnType(cacheColumn: CacheColumn): Promise<string> {
        let {expression} = cacheColumn.select.columns[0];

        const explicitCastType = expression.getExplicitCastType();
        if ( explicitCastType ) {
            return explicitCastType;
        }

        if ( expression.isCoalesce() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;
            expression = funcCall.args[0] as Expression;
        }

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;
            if ( funcCall.name === "max" || funcCall.name === "min" ) {
                expression = funcCall.args[0] as Expression;
            }
        }

        if ( expression.isNotExists() ) {
            return "boolean";
        }

        if ( expression.isCaseWhen() ) {
            const firstThen = expression.getFirstNotNullThenExpression();
            if ( firstThen ) {
                return this.getColumnType(
                    cacheColumn.replaceExpression(firstThen)
                );
            }
        }

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0] as FuncCall;

            if ( funcCall.name === "count" ) {
                return "bigint";
            }

            if ( funcCall.name === "string_agg" ) {
                return "text";
            }

            if ( funcCall.name === "sum" ) {
                return "numeric";
            }

            if ( funcCall.name === "bool_or" || funcCall.name === "bool_and" ) {
                return "boolean";
            }

            if ( funcCall.name === "array_agg" ) {
                const firstArg = funcCall.args[0] as Expression;
                const columnRef = firstArg.getColumnReferences()[0];
    
                if ( columnRef && firstArg.elements.length === 1 ) {        
                    const dbColumn = this.findDbColumnByRef(columnRef);
                    
                    if ( dbColumn ) {
                        return dbColumn.type + "[]";
                    }

                    if ( columnRef.name === "id" ) {
                        return "integer[]";
                    }
                }
            }

            const newFunc = this.migration.toCreate.functions.find(func =>
                func.equalName(funcCall.name)
            );
            if ( newFunc && newFunc.returns.type ) {
                return newFunc.returns.type;
            }
        }

        if ( expression.isColumnReference() ) {
            const columnRef = expression.elements[0] as ColumnReference;
            const dbColumn = this.findDbColumnByRef(columnRef);
            if ( dbColumn ) {
                return dbColumn.type.toString();
            }
        }

        if ( expression.isArrayItemOfColumnReference() ) {
            const columnRef = expression.elements[0] as ColumnReference;
            const dbColumn = this.findDbColumnByRef(columnRef);

            if ( dbColumn && dbColumn.type.isArray() ) {
                const arrayType = dbColumn.type.toString();
                const elemType = arrayType.slice(0, -2);
                return elemType;
            }
        }

        const selectWithReplacedColumns = await this.replaceUnknownColumns(cacheColumn.select);
        const columnsTypes = await this.driver.getCacheColumnsTypes(
            new Select({
                columns: [
                    selectWithReplacedColumns.columns[0]
                ],
                from: selectWithReplacedColumns.from
            }),
            cacheColumn.for
        );

        const columnType = Object.values(columnsTypes)[0];
        return columnType;
    }

    private findDbColumnByRef(columnRef: ColumnReference) {
        const dbTable = this.database.getTable(
            columnRef.tableReference.table
        );
        const dbColumn = dbTable && dbTable.getColumn(columnRef.name);

        if ( dbColumn ) {
            return dbColumn;
        }

        const maybeIsCreatingNow = this.migration.toCreate.columns.find(newColumn =>
            newColumn.equalName(columnRef) &&
            newColumn.table.equal(columnRef.tableReference.table)
        );
        if ( maybeIsCreatingNow ) {
            return maybeIsCreatingNow;
        }
    }

    private async replaceUnknownColumns(select: Select) {
        let replacedSelect = select;

        for (const selectColumn of select.columns) {
            const columnRefs = selectColumn.expression.getColumnReferences();
            for (const columnRef of columnRefs) {
                const dbTable = this.database.getTable(
                    columnRef.tableReference.table
                );
                const dbColumn = dbTable && dbTable.getColumn(columnRef.name);
                if ( dbColumn ) {
                    continue;
                }

                const maybeIsCreatingNow = this.migration.toCreate.columns.find(newColumn =>
                    newColumn.equalName(columnRef) &&
                    newColumn.table.equal(columnRef.tableReference.table)
                );
                if ( maybeIsCreatingNow ) {
                    replace(
                        columnRef,
                        maybeIsCreatingNow.type.toString()
                    );
                    continue;
                }

                const toUpdateThatColumn = this.graph.getColumns(columnRef.tableReference)
                    .find(cacheColumn => cacheColumn.name === columnRef.name);
                if ( toUpdateThatColumn ) {
                    const newColumnType = await this.getColumnType(toUpdateThatColumn);

                    replace(
                        columnRef,
                        newColumnType
                    );
                }
            }
        }

        function replace(
            columnRef: ColumnReference,
            columnType: string
        ) {
            replacedSelect = replacedSelect.clone({
                columns: replacedSelect.columns.map(selectColumn => {
                    const newExpression = selectColumn.expression.replaceColumn(
                        columnRef,
                        UnknownExpressionElement.fromSql(
                            `(null::${ columnType })`
                        )
                    );

                    return selectColumn.replaceExpression(newExpression);
                })
            });
        }

        return replacedSelect;
    }

    private getColumnDefault(select: Select) {
        const selectColumn = select.columns[0] as SelectColumn;

        if ( selectColumn.expression.isThatFuncCall("count") ) {
            return "0";
        }
        if ( selectColumn.expression.isNotExists() ) {
            return "false";
        }

        if ( selectColumn.expression.isThatFuncCall("coalesce") ) {
            const [coalesce] = selectColumn.expression.getFuncCalls();
            const lastArg = coalesce.getLastArg()!;
            if ( lastArg.isPrimitive() ) {
                return lastArg.toString();
            }
        }

        return "null";
    }
}
