import {
    Update, SetItem, SetSelectItem,
    Expression,
    SelectColumn,
    Spaces,
    ColumnReference
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByArrayReferenceBody } from "../body/buildOneLastRowByArrayReferenceBody";
import { flatMap } from "lodash";
import { TableReference } from "../../../database/schema/TableReference";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import assert from "assert";

export class LastRowByArrayReferenceTriggerBuilder extends AbstractLastRowTriggerBuilder {

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

        const lastIdColumnName = this.helperColumnName("id");
        const orderBy = this.context.cache.select.orderBy[0]!;

        const updateOnInsert = new Update({
            table: this.context.cache.for.toString(),
            set: [
                ...this.setHelpersByNewRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( new.${ arrColumnRef.name } )`,
                ...this.whereIsGreat(),
                this.whereDistinctRowValues("new")
            ])
        });

        const updateOnDelete = new Update({
            table: this.context.cache.for.toString(),
            set: [
                this.reselectSetItem()
            ],
            where: Expression.and([
                `${cacheTable}.id = any( old.${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = old.id`
            ])
        });

        const updateNotChangedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                ...this.setHelpersByNewRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( not_changed_${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = new.id`,
                this.whereDistinctRowValues("new")
            ])
        });
        const updateNotChangedIdsWithReselect = new Update({
            table: this.context.cache.for.toString(),
            set: [
                this.reselectSetItem()
            ],
            where: Expression.unknown(
                `${cacheTable}.id = any( not_changed_${ arrColumnRef.name } )`
            )
        });
        const updateDeletedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                this.reselectSetItem()
            ],
            where: Expression.and([
                `${cacheTable}.id = any( deleted_${ arrColumnRef.name } )`,
                `${cacheTable}.${ lastIdColumnName } = new.id`
            ])
        });
        const updateInsertedIds = new Update({
            table: this.context.cache.for.toString(),
            set: [
                ...this.setHelpersByNewRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( inserted_${ arrColumnRef.name } )`,
                ...(
                    this.isOrderById() ?
                        [Expression.or([
                            `${cacheTable}.${ lastIdColumnName } is null`,
                            `${cacheTable}.${ lastIdColumnName } ${
                                orderBy.type == "asc" ? ">" : "<"
                            } new.id`
                        ])] : 
                        this.whereIsGreat()
                ),
                this.whereDistinctRowValues("new")
            ])
        });

        const body = buildOneLastRowByArrayReferenceBody({
            needMatching: this.context.referenceMeta.filters.length > 0,
            arrColumn,
            orderByColumnName: this.getOrderByColumnRef().name,
            updateNotChangedIdsWithReselect,
            dataFields: this.findDataColumns(),
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

    private setHelpersByNewRow() {
        const helpers: SetItem[] = this.getOrderByColumnsRefs().map(columnRef =>
            new SetItem({
                column: this.helperColumnName(columnRef.name),
                value: Expression.unknown("new." + columnRef.name)
            })
        );
        return helpers;
    }

    private getOrderByColumnsRefs() {
        // TODO: order by hard expression
        const orderBy = this.context.cache.select.orderBy[0]!;
        const orderByColumns = orderBy.expression.getColumnReferences();
        
        const hasId = orderByColumns.some(columnRef => columnRef.name === "id");
        if ( !hasId ) {
            orderByColumns.unshift(
                new ColumnReference(
                    this.fromTable(),
                    "id"
                )
            );
        }

        return orderByColumns;
    }

    private whereIsGreat() {
        const cacheTable = (
            this.context.cache.for.alias ||
            this.context.cache.for.table.toStringWithoutPublic()
        );
        const lastIdColumnName = this.helperColumnName("id");

        if ( this.isOrderById() ) {
            const orderBy = this.context.cache.select.orderBy[0]!;
            if ( orderBy.type === "asc" ) {
                return [
                    `${cacheTable}.${lastIdColumnName} is null`
                ];
            }
            return [];
        }

        const firstOrderColumn = this.getOrderByColumnRef();
        return [Expression.or([
            `${cacheTable}.${lastIdColumnName} is null`,
            Expression.and([
                `new.${firstOrderColumn.name} is null`,
                `${cacheTable}.${this.helperColumnName(firstOrderColumn.name)} is not null`
            ]),
            `${cacheTable}.${this.helperColumnName(firstOrderColumn.name)} < new.${firstOrderColumn.name}`
        ])];
    }

    private helperColumnName(triggerTableColumnName: string) {
        return `__${this.context.cache.name}_${triggerTableColumnName}`;
    }

    private isOrderById() {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const orderByColumns = orderBy.expression.getColumnReferences();
        const firstOrderColumn = this.getOrderByColumnRef();
        const byId = (
            orderByColumns.length === 1 &&
            firstOrderColumn.name === "id" &&
            this.context.isColumnRefToTriggerTable( firstOrderColumn )
        );

        return byId;
    }

    private getOrderByColumnRef() {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const orderByColumns = orderBy.expression.getColumnReferences();
        const firstOrderColumn = orderByColumns[0];
        return firstOrderColumn;
    }

    private reselectSetItem() {
        return new SetSelectItem({
            columns: [
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    this.helperColumnName(columnRef.name)
                ),
                ...this.context.cache.select.columns.map(selectColumn =>
                    selectColumn.name
                )
            ],
            select: this.reselect()
                .toSQL( Spaces.level(3) )
                .trim()
        });
    }

    private reselect() {
        const select = this.context.cache.select.cloneWith({
            columns: [
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    new SelectColumn({
                        name: this.helperColumnName(columnRef.name),
                        expression: Expression.unknown(
                            this.triggerTableAlias() + "." + columnRef.name
                        )
                    })
                ),
                ...this.context.cache.select.columns
            ]
        });
        return select;
    }
}
