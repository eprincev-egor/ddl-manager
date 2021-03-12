import {
    Update, Expression, SetItem,
    SelectColumn,
    ColumnReference, UnknownExpressionElement, SimpleSelect
} from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildOneLastRowBody } from "./body/buildOneLastRowBody";

export class OneLastRowTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const isLastColumn = [
            "_",
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name
        ].join("_");

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
                column: isLastColumn,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_row.id`
            ])
        });

        const updateMaxRowLastColumnFalse = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumn,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                `${triggerTable}.id = max_prev_id`,
                `${isLastColumn} = true`
            ])
        });

        const updateThisRowLastColumnTrue = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumn,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = new.id`
            ])
        });

        const clearLastColumnOnInsert = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumn,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                `${triggerTable}.id < new.id`,
                `${isLastColumn} = true`
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

        const selectMaxPrevId = new SimpleSelect({
            columns: [`max( ${ triggerTable }.id )`],
            from: this.context.triggerTable,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                `${triggerTable}.id <> new.id`
            ])
        });

        const selectPrevRow = this.context.cache.select.cloneWith({
            columns: selectPrevRowColumns,
            where: Expression.and(
                this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = old.${column}`
                )
            ),
            intoRow: "prev_row"
        });

        const body = buildOneLastRowBody({
            isLastColumn,
            updateNew,

            selectMaxPrevId,
            selectPrevRow,
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
                UnknownExpressionElement.fromSql(`old.${isLastColumn}`)
            ]),
            noChanges: this.conditions.noChanges(),
            noReferenceChanges: this.conditions.noReferenceChanges()
        });
        return body;
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