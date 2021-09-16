import {
    Update, SetItem,
    Expression,
    SelectColumn,
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
        const select = this.context.cache.select.cloneWith({
            columns: this.getOrderByColumnsRefs().map(columnRef =>
                new SelectColumn({
                    name: this.helperColumnName(columnRef.name),
                    expression: new Expression([
                        new ColumnReference(
                            this.fromTable(),
                            columnRef.name
                        )
                    ])
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
            orderByColumnName: orderBy.getFirstColumnRef()!.name,
            updateNotChangedIdsWithReselect,
            dataFields: this.findDataColumns(),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            noChanges: this.conditions.noChanges(),
            newSortIsGreat: orderBy.compareRowsByOrder("new", "above", "old"),
            noOrderChanges: Expression.and(
                orderBy.getColumnReferences().map(columnRef =>
                    `new.${columnRef.name} is not distinct from old.${columnRef.name}`
                )
            ),
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

}
