import { Column } from "../database/schema/Column";
import { Comment } from "../database/schema/Comment";
import {
    Select,
    SelectColumn,
    Expression,
    ColumnReference,
    FuncCall
} from "../ast";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { CacheColumnGraph } from "./graph/CacheColumnGraph";
import { CacheColumn } from "./graph/CacheColumn";
import { FilesState } from "../fs/FilesState";

export interface CacheColumnBuilderParams {
    driver: IDatabaseDriver;
    database: Database;
    fs: FilesState;
    graph: CacheColumnGraph;
}

export class CacheColumnBuilder {

    private driver: IDatabaseDriver;
    private fs: FilesState;
    private database: Database;
    private graph: CacheColumnGraph;
    private knownTypes: Record<string, string>;

    constructor(params: CacheColumnBuilderParams) {
        this.driver = params.driver;
        this.database = params.database;
        this.fs = params.fs;
        this.graph = params.graph;
        this.knownTypes = {};
    }

    async build(cacheColumn: CacheColumn) {
        const columnToCreate = new Column(
            cacheColumn.for.table,
            cacheColumn.name,
            await this.getColumnType(cacheColumn),
            getColumnDefault(cacheColumn.select),
            Comment.fromFs({
                objectType: "column",
                cacheSignature: cacheColumn.cache.signature,
                cacheSelect: cacheColumn.select.toString()
            })
        );
        return columnToCreate;
    }

    private async getColumnType(cacheColumn: CacheColumn): Promise<string> {
        const knownType = this.knownTypes[ cacheColumn.getId() ];
        if ( knownType ) {
            return knownType;
        }

        const calculatedType = this.tryDetectTypeByExpression(
            cacheColumn.getColumnExpression()
        );
        if ( calculatedType ) {
            this.knownTypes[ cacheColumn.getId() ] = calculatedType;
            return calculatedType;
        }

        this.knownTypes[ cacheColumn.getId() ] = "!recursion!";

        const expression = await this.replaceDeps(
            cacheColumn.getColumnExpression()
        );
        const loadedType = await this.driver.getType(expression);

        this.knownTypes[ cacheColumn.getId() ] = loadedType;
        return loadedType;
    }

    private async replaceDeps(expression: Expression) {
        for (const funcCall of expression.getFuncCalls()) {
            const type = this.tryDetectFuncCallType(funcCall);
            if ( type ) {
                expression = expression.replaceFuncCall(
                    funcCall, `(null::${type})`
                );
            }
        }

        for (const columnRef of expression.getColumnReferences()) {
            const typeExpression = await this.calculateColumnRefTypeExpression(columnRef);

            expression = expression.replaceColumn(
                columnRef,
                Expression.unknown(`(${typeExpression})`)
            );
        }

        const unknownAggregations = expression.getFuncCalls().filter(funcCall =>
            this.database.aggregators.includes(funcCall.name)
        );
        for (const aggCall of unknownAggregations) {
            expression = expression.replaceFuncCall(
                aggCall, `(select ${aggCall})`
            );
        }

        return expression;
    }

    private async calculateColumnRefTypeExpression(columnRef: ColumnReference) {
        const dbColumn = this.database.getColumn(
            columnRef.tableReference.table,
            columnRef.name
        );

        if ( columnRef.name === "id" ) {
            return `null::${dbColumn?.type || "integer"}`;
        }

        const cacheColumn = this.graph.getColumn(
            columnRef.tableReference,
            columnRef.name
        );
        if ( cacheColumn ) {
            const type = await this.getColumnType(cacheColumn);
            if ( type === "!recursion!" ) {
                return "null";
            }

            return `null::${type}`;
        }

        if ( dbColumn ) {
            return `null::${dbColumn.type}`;
        }

        throw new Error(`table "${columnRef.tableReference.table}" does not have column "${columnRef.name}"`);
    }

    private tryDetectTypeByExpression(expression: Expression): string | undefined {
        const explicitCastType = expression.getExplicitCastType();
        if ( explicitCastType ) {
            return explicitCastType;
        }

        if ( expression.isFuncCall() ) {
            const funcCall = expression.getFuncCalls()[0];
            return this.tryDetectFuncCallType(funcCall);
        }

        if ( expression.isColumnReference() ) {
            const columnRef = expression.getColumnReferences()[0];

            const dbColumn = this.database.getColumn(
                columnRef.tableReference.table,
                columnRef.name
            );
            if ( dbColumn?.isFrozen() ) {
                return dbColumn.type.toString();
            }

            if ( columnRef.name === "id" ) {
                return "integer";
            }
        }

        if ( expression.isNotExists() ) {
            return "boolean";
        }
    }

    private tryDetectFuncCallType(funcCall: FuncCall) {
        const funcName = funcCall.getOnlyName();
        const firstArg = funcCall.getFirstArg();

        if ( funcName === "string_agg" ) {
            return "text";
        }

        if ( funcName === "sum" ) {
            return "numeric";
        }

        if ( funcName === "max_or_null_date_agg" || funcName === "min_or_null_date_agg" ) {
            return "timestamp without time zone";
        }

        if ( ["min", "max", "first", "last", "coalesce"].includes(funcName) ) {
            return firstArg ? this.tryDetectTypeByExpression(firstArg) : undefined;
        }

        if ( funcName === "count" ) {
            return "bigint";
        }

        if ( ["bool_or", "bool_and", "every"].includes(funcName) ) {
            return "boolean";
        }

        if ( funcName === "array_agg" ) {
            const argType = firstArg && this.tryDetectTypeByExpression(firstArg);
            return argType ? `${argType}[]` : undefined;
        }

        const fsFuncs = this.fs.getFunctionsByName(funcName);
        if ( fsFuncs.length === 1 && fsFuncs[0].returns.type ) {
            return fsFuncs[0].returns.type;
        }
    }
}

function getColumnDefault(select: Select) {
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
