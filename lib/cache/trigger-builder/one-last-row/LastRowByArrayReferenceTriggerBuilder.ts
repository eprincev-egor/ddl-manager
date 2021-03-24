import {
    Update, SetItem, SetSelectItem,
    Expression,
    SelectColumn,
    Spaces,
    ColumnReference,
    OrderByItem,
    OrderBy
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByArrayReferenceBody } from "../body/buildOneLastRowByArrayReferenceBody";
import { flatMap } from "lodash";
import { TableReference } from "../../../database/schema/TableReference";
import { CoalesceFalseExpression } from "../../../ast/expression/CoalesceFalseExpression";
import assert from "assert";

export class LastRowByArrayReferenceTriggerBuilder extends AbstractLastRowTriggerBuilder {

    createSelectForUpdateHelperColumn() {
        const select = this.context.cache.select.cloneWith({
            columns: this.getOrderByColumnsRefs().map(columnRef =>
                new SelectColumn({
                    name: this.helperColumnName(columnRef.name),
                    expression: Expression.unknown(
                        this.triggerTableAlias() + "." + columnRef.name
                    )
                })
            )
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
        const orderBy = this.context.cache.select.orderBy!;

        const updateOnInsert = new Update({
            table: this.context.cache.for.toString(),
            set: [
                ...this.setHelpersByRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( new.${ arrColumnRef.name } )`,
                ...this.whereIsGreat()
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
                ...this.setHelpersByRow(),
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
                ...this.setHelpersByRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( inserted_${ arrColumnRef.name } )`,
                ...(
                    orderBy.isOnlyId() ?
                        [Expression.or([
                            `${cacheTable}.${ lastIdColumnName } is null`,
                            `${cacheTable}.${ lastIdColumnName } ${
                                orderBy.items[0]!.type == "asc" ? ">" : "<"
                            } new.id`
                        ])] : 
                        this.whereIsGreat()
                )
            ])
        });

        const orderByColumnName = orderBy.getFirstColumnRef()!.name;
        const newSortIsGreat = Expression.or([
            Expression.and([
                `new.${orderByColumnName} is null`,
                `old.${orderByColumnName} is not null`
            ]),
            `new.${orderByColumnName} > old.${orderByColumnName}`
        ]);

        const updateNotChangedIdsWhereSortIsLess = new Update({
            table: this.context.cache.for.toString(),
            set: [
                ...this.setHelpersByRow(),
                ...this.setItemsByRow("new")
            ],
            where: Expression.and([
                `${cacheTable}.id = any( not_changed_${ arrColumnRef.name } )`,
                ...this.whereIsGreat([
                    Expression.unknown(
                        `${cacheTable}.${lastIdColumnName} = new.id`
                    )
                ])
            ])
        });

        const body = buildOneLastRowByArrayReferenceBody({
            needMatching: this.context.referenceMeta.filters.length > 0,
            arrColumn,
            orderByColumnName,
            updateNotChangedIdsWithReselect,
            dataFields: this.findDataColumns(),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            noChanges: this.conditions.noChanges(),
            newSortIsGreat,
            updateNotChangedIdsWhereSortIsLess,
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

    private setHelpersByRow(row = "new") {
        const helpers: SetItem[] = this.getOrderByColumnsRefs().map(columnRef =>
            new SetItem({
                column: this.helperColumnName(columnRef.name),
                value: Expression.unknown(row + "." + columnRef.name)
            })
        );
        return helpers;
    }

    private getOrderByColumnsRefs() {
        // TODO: order by hard expression
        const {select} = this.context.cache;
        const orderByColumns = select.orderBy!.getColumnReferences();
        
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

    private whereIsGreat(additionalOr: Expression[] = []) {
        const cacheTable = (
            this.context.cache.for.alias ||
            this.context.cache.for.table.toStringWithoutPublic()
        );
        const orderBy = this.context.cache.select.orderBy!;
        const firstOrderColumn = orderBy.getFirstColumnRef()!;
        const lastIdColumnName = this.helperColumnName("id");
        const lastSortColumnName = this.helperColumnName(firstOrderColumn.name);

        if ( orderBy.isOnlyId() ) {
            if ( orderBy.items[0]!.type === "asc" ) {
                return [
                    `${cacheTable}.${lastIdColumnName} is null`
                ];
            }
            return [];
        }

        return [Expression.or([
            ...additionalOr,
            `${cacheTable}.${lastIdColumnName} is null`,
            Expression.and([
                `${cacheTable}.${lastSortColumnName} is not distinct from new.${firstOrderColumn.name}`,
                `${cacheTable}.${lastIdColumnName} ${
                    orderBy.items[0]!.type == "asc" ? ">" : "<"
                } new.id`
            ]),
            Expression.and([
                `new.${firstOrderColumn.name} is null`,
                `${cacheTable}.${lastSortColumnName} is not null`
            ]),
            `${cacheTable}.${lastSortColumnName} ${
                orderBy.items[0]!.type == "asc" ? ">" : "<"
            } new.${firstOrderColumn.name}`
        ])];
    }

    private helperColumnName(triggerTableColumnName: string) {
        return `__${this.context.cache.name}_${triggerTableColumnName}`;
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
        const {select} = this.context.cache;
        const orderByItems = select.orderBy!.items.slice();
        if ( !select.orderBy!.isOnlyId() ) {
            const firstOrder = orderByItems[0]!;
            orderByItems.push(
                new OrderByItem({
                    expression: Expression.unknown(
                        `${this.fromTable().getIdentifier()}.id`
                    ),
                    type: firstOrder.type,
                    nulls: firstOrder.nulls
                })
            );
        }

        const reselect = select.cloneWith({
            columns: [
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    new SelectColumn({
                        name: this.helperColumnName(columnRef.name),
                        expression: Expression.unknown(
                            this.triggerTableAlias() + "." + columnRef.name
                        )
                    })
                ),
                ...select.columns
            ],
            orderBy: new OrderBy(orderByItems)
        });
        return reselect;
    }
}
