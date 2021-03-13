import {
    Update, Expression, SetItem,
    Select, SelectColumn,
    ColumnReference, UnknownExpressionElement, 
    SimpleSelect,
    Spaces,
    From
} from "../../ast";
import { Exists } from "../../ast/expression/Exists";
import { Comment } from "../../database/schema/Comment";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractTriggerBuilder, ICacheTrigger } from "./AbstractTriggerBuilder";
import { buildOneLastRowBody } from "./body/buildOneLastRowBody";

export class OneLastRowTriggerBuilder extends AbstractTriggerBuilder {

    createSelectForUpdateHelperColumn() {
        const fromTable = this.context.triggerTable;
        const fromRef = new TableReference(fromTable);
        const prevRef = new TableReference(
            fromTable,
            `prev_${ fromTable.name }`
        );
        const orderBy = this.context.cache.select.orderBy[0]!;

        const select = new Select({
            columns: [new SelectColumn({
                name: this.getIsLastColumnName(),
                expression: new Expression([
                    UnknownExpressionElement.fromSql(`not`),
                    new Exists({
                        select: new Select({
                            columns: [],
                            from: [
                                new From(prevRef)
                            ],
                            where: Expression.and([
                                ...this.context.referenceMeta.columns.map(column =>
                                    new Expression([
                                        new ColumnReference(prevRef, column),
                                        UnknownExpressionElement.fromSql("="),
                                        new ColumnReference(fromRef, column),
                                    ])
                                ),
                                ...this.context.referenceMeta.filters,
                                new Expression([
                                    new ColumnReference(prevRef, "id"),
                                    UnknownExpressionElement.fromSql(
                                        orderBy.type === "desc" ? 
                                            ">" : "<"
                                    ),
                                    new ColumnReference(fromRef, "id"),
                                ])
                            ])
                        })
                    })
                ])
            })],
            from: []
        });
        return select;
    }

    createHelperTrigger(): ICacheTrigger | undefined {
        const orderBy = this.context.cache.select.orderBy[0]!;
        if ( orderBy.type === "asc" ) {
            return;
        }

        const isLastColumnName = this.getIsLastColumnName();

        const helperTriggerName = [
            "cache",
            this.context.cache.name,
            "before",
            "insert",
            this.context.triggerTable.name
        ].join("_");

        const trigger = new DatabaseTrigger({
            name: helperTriggerName,
            before: true,
            insert: true,
            procedure: {
                schema: "public",
                name: helperTriggerName,
                args: []
            },
            table: this.context.triggerTable,
            comment: Comment.fromFs({
                objectType: "trigger",
                cacheSignature: this.context.cache.getSignature()
            })
        });

        return {
            trigger,
            function: new DatabaseFunction({
                schema: "public",
                name: helperTriggerName,
                body: `
    begin

    new.${isLastColumnName} = (
        ${this.conditions
            .hasReferenceWithoutJoins("new")!
            .toSQL( Spaces.level(2) )}
    );

    return new;
    end;
                `.trim(),
                comment: Comment.fromFs({
                    objectType: "function",
                    cacheSignature: this.context.cache.getSignature()
                }),
                args: [],
                returns: {type: "trigger"}
            })
        };
    }

    protected createBody() {
        const isLastColumnName = this.getIsLastColumnName();

        const updateNew = new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("new"),
            where: Expression.and([
                this.conditions.simpleWhere("new")!,
                this.whereDistinctRowValues("new")
            ])
        });
        const updatePrev = new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("prev_row"),
            where: Expression.and([
                this.conditions.simpleWhere("old")!,
                this.whereDistinctRowValues("prev_row")
            ])
        });

        const triggerTable = this.context.triggerTable.toStringWithoutPublic();
        const updatePrevRowLastColumnTrue = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_row.id`
            ])
        });

        const updateMaxRowLastColumnFalse = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_id`,
                `${isLastColumnName} = true`
            ])
        });

        const updateThisRowLastColumnTrue = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = new.id`
            ])
        });

        const clearLastColumnOnInsert = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                `${triggerTable}.id < new.id`,
                `${isLastColumnName} = true`
            ])
        });

        const selectPrevRowColumnsNames = this.context.triggerTableColumns.slice();
        if  ( !selectPrevRowColumnsNames.includes("id") ) {
            selectPrevRowColumnsNames.unshift("id");
        }

        const selectPrevRowColumns: SelectColumn[] = selectPrevRowColumnsNames.map(name =>
            new SelectColumn({
                name,
                expression: Expression.unknown(name)
            })
        );

        const orderBy = this.context.cache.select.orderBy[0]!;
        const selectMaxPrevId = new SimpleSelect({
            columns: [
                `${ orderBy.type == "desc" ? "max" : "min" }( ${ triggerTable }.id )`
            ],
            from: this.context.triggerTable,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                ...this.context.referenceMeta.filters,
                `${triggerTable}.id <> new.id`
            ])
        });

        const selectPrevRow = this.context.cache.select.cloneWith({
            columns: selectPrevRowColumns,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = old.${column}`
                ),
                ...this.context.referenceMeta.filters
            ]),
            intoRow: "prev_row"
        });
        const existsPrevRow = new Exists({
            select: this.context.cache.select.cloneWith({
                columns: [],
                where: Expression.and([
                    ...this.context.referenceMeta.columns.map(column =>
                        `${triggerTable}.${column} = new.${column}`
                    ),
                    ...this.context.referenceMeta.filters,
                    `${triggerTable}.id < new.id`
                ]),
                orderBy: [],
                limit: undefined
            })
        });

        const body = buildOneLastRowBody({
            orderVector: orderBy.type,

            isLastColumn: isLastColumnName,
            ifNeedUpdateNewOnChangeReference: Expression.or([
                `prev_id ${orderBy.type === "desc" ? "<" : ">"} new.id`,
                "prev_id is null"
            ]),
            updateNew,

            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf(),

            selectMaxPrevId,
            selectPrevRow,
            existsPrevRow,
            updatePrev,

            updateMaxRowLastColumnFalse,
            updateThisRowLastColumnTrue,

            clearLastColumnOnInsert,
            updatePrevRowLastColumnTrue,

            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                UnknownExpressionElement.fromSql(`old.${isLastColumnName}`)
            ]),
            noChanges: this.conditions.noChanges(),
            noReferenceChanges: this.conditions.noReferenceChanges()
        });
        return body;
    }

    private getIsLastColumnName() {
        const helperColumnName = [
            "_",
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name
        ].join("_");
        return helperColumnName;
    }

    private whereDistinctRowValues(row: string) {
        const selectColumns = this.context.cache.select.columns;
        const prevValues = selectColumns.map(selectColumn => 
            this.replaceTriggerTableToRow(
                row, selectColumn.expression
            )
        );
        return this.whereDistinctFrom(prevValues);
    }

    private whereDistinctFrom(values: Expression[]) {
        const selectColumns = this.context.cache.select.columns;
        const compareConditions = selectColumns.map((selectColumn, i) => {
            const cacheColumnRef = new ColumnReference(
                this.context.cache.for,
                selectColumn.name
            );
            return UnknownExpressionElement.fromSql(
                `${cacheColumnRef} is distinct from ${ values[ i ] }`
            );
        });
        return Expression.or(compareConditions);
    }

    private setItemsByRow(row: string) {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: this.replaceTriggerTableToRow(
                    row, selectColumn.expression
                )
            })
            return setItem;
        });
        return setItems;
    }
}