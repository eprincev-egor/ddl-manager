import {
    Expression, SetItem,
    Select, SelectColumn,
    ColumnReference, UnknownExpressionElement,
    From
} from "../../../ast";
import { Exists } from "../../../ast/expression/Exists";
import { TableReference } from "../../../database/schema/TableReference";
import { AbstractTriggerBuilder, ICacheTrigger } from "../AbstractTriggerBuilder";

export abstract class AbstractLastRowTriggerBuilder extends AbstractTriggerBuilder {

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
        return;
    }

    protected getIsLastColumnName() {
        const helperColumnName = [
            "_",
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name
        ].join("_");
        return helperColumnName;
    }

    protected whereDistinctRowValues(row: string) {
        const selectColumns = this.context.cache.select.columns;
        const prevValues = selectColumns.map(selectColumn => 
            this.replaceTriggerTableToRow(
                row, selectColumn.expression
            )
        );
        return this.whereDistinctFrom(prevValues);
    }

    protected whereDistinctFrom(values: Expression[]) {
        const selectColumns = this.context.cache.select.columns;
        const compareConditions = selectColumns.map((selectColumn, i) => {
            // TODO: test hard expressions
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

    protected setItemsByRow(row: string) {
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