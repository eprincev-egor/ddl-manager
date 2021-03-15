import {
    Update, SetItem, SetSelectItem,
    Expression,
    SelectColumn,
    Spaces
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByIdAndArrayReferenceBody } from "../body/buildOneLastRowByIdAndArrayReferenceBody";
import { flatMap } from "lodash";
import { TableReference } from "../../../database/schema/TableReference";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import assert from "assert";

export class LastRowByIdAndArrayReferenceTriggerBuilder extends AbstractLastRowTriggerBuilder {

    createSelectForUpdateHelperColumn() {
        const lastIdColumnName = "__" + this.context.cache.name + "_id";
        const select = this.context.cache.select.cloneWith({
            columns: [
                new SelectColumn({
                    name: lastIdColumnName,
                    expression: Expression.unknown(
                        this.triggerTableAlias() + ".id"
                    )
                })
            ]
        });
        return {select, for: this.context.cache.for};
    }

    protected createBody() {
        const dbTable = this.context.database
            .getTable(this.context.triggerTable);
        // TODO: check other columns
        const columnsRefs = flatMap(
            this.context.referenceMeta.expressions,
            expression => expression.isBinary("&&") ?
                expression.getColumnReferences() :
                []
        );
        const arrColumnRef = columnsRefs.find(columnRef =>
            this.context.isColumnRefToTriggerTable(columnRef)
        )!;
        const arrColumn = dbTable && dbTable.getColumn(arrColumnRef.name);
        assert.ok(arrColumn, "required db column with array type: " + arrColumnRef.toString());
    
        const cacheTable = (
            this.context.cache.for.alias ||
            this.context.cache.for.table.toStringWithoutPublic()
        );

        const lastIdColumnName = "__" + this.context.cache.name + "_id";

        const updateOnInsert = new Update({
            table: this.context.cache.for.toString(),
            set: [
                new SetItem({
                    column: lastIdColumnName,
                    value: Expression.unknown("new.id")
                }),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( new.${ arrColumnRef.name } )`,
                this.whereDistinctRowValues("new")
            ])
        });

        const updateOnDelete = new Update({
            table: this.context.cache.for.toString(),
            set: [
                new SetSelectItem({
                    columns: [
                        lastIdColumnName,
                        ...this.context.cache.select.columns.map(selectColumn =>
                            selectColumn.name
                        )
                    ],
                    select: this.context.cache.select.cloneWith({
                        columns: [
                            new SelectColumn({
                                name: lastIdColumnName,
                                expression: Expression.unknown(
                                    this.triggerTableAlias() + ".id"
                                )
                            }),
                            ...this.context.cache.select.columns
                        ]
                    }).toSQL( Spaces.level(3) ).trim()
                })
            ],
            where: Expression.and([
                `${cacheTable}.id = any( old.${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = old.id`
            ])
        });

        const updateNotChangedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                new SetItem({
                    column: lastIdColumnName,
                    value: Expression.unknown("new.id")
                }),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( not_changed_${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = new.id`,
                this.whereDistinctRowValues("new")
            ])
        });
        const updateDeletedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                new SetSelectItem({
                    columns: [
                        lastIdColumnName,
                        ...this.context.cache.select.columns.map(selectColumn =>
                            selectColumn.name
                        )
                    ],
                    select: this.context.cache.select.cloneWith({
                        columns: [
                            new SelectColumn({
                                name: lastIdColumnName,
                                expression: Expression.unknown(
                                    this.triggerTableAlias() + ".id"
                                )
                            }),
                            ...this.context.cache.select.columns
                        ]
                    }).toSQL( Spaces.level(3) ).trim()
                })
            ],
            where: Expression.and([
                `${cacheTable}.id = any( deleted_${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = new.id`
            ])
        });
        const updateInsertedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                new SetItem({
                    column: lastIdColumnName,
                    value: Expression.unknown("new.id")
                }),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( inserted_${ arrColumnRef.name } )`,
                Expression.or([
                    `${cacheTable}.${ lastIdColumnName } is null`,
                    // TODO: check order vector asc/desc
                    `${cacheTable}.${ lastIdColumnName } < new.id`
                ]),
                this.whereDistinctRowValues("new")
            ])
        });

        const body = buildOneLastRowByIdAndArrayReferenceBody({
            arrColumn,
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            noChanges: this.conditions.noChanges(),
            updateOnInsert,
            updateOnDelete,
            updateNotChangedIds,
            updateDeletedIds,
            updateInsertedIds,
            matchedNew: this.matched("new"),
            matchedOld: this.matched("old")
        });
        return body;
    }

    private matched(row: string) {
        const matchedExpression = new CoalesceFalseExpression(
            Expression.and(
                this.context.referenceMeta.filters
            ).replaceTable(
                this.context.triggerTable,
                new TableReference(this.context.triggerTable, row)
            )
        );
        return matchedExpression;
    }
}