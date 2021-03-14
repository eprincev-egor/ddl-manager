import {
    Expression, ConditionElementType,
    Update, SetItem, 
    SelectColumn
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByMutableBody } from "../body/buildOneLastRowByMutableBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    protected createBody() {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const sortColumnRef = orderBy.expression.getColumnReferences()[0]!;

        const triggerTable = this.context.triggerTable.toStringWithoutPublic();
        const isLastColumnName = this.getIsLastColumnName();

        const selectPrevRowByFlag = this.context.cache.select.cloneWith({
            columns: [
                SelectColumn.onlyName("id"),
                SelectColumn.onlyName(sortColumnRef.name)
            ],
            where: this.filterTriggerTable("new", [
                // TODO: check alias
                `${triggerTable}.${isLastColumnName} = true`
            ]),
            orderBy: [],
            limit: undefined,
            intoRow: "prev_row"
        });

        const selectPrevRowWhereGreatOrder = this.context.cache.select.cloneWith({
            columns: this.allPrevRowColumns(),
            where: this.filterTriggerTable("new", [
                // TODO: check alias
                `${triggerTable}.${sortColumnRef.name} > new.${sortColumnRef.name}`
            ]),
            intoRow: "prev_row"
        });

        const body = buildOneLastRowByMutableBody({
            isLastColumn: isLastColumnName,
            noReferenceAndSortChanges: Expression.and([
                this.conditions.noReferenceChanges(),
                `new.${sortColumnRef.name} is not distinct from old.${sortColumnRef.name}`
            ]),
            isLastAndHasDataChange: Expression.and([
                Expression.unknown(`new.${isLastColumnName}`),
                Expression.or(
                    this.findDataColumns().map(columnName =>
                        `new.${columnName} is distinct from old.${columnName}`
                    )
                )
            ]),
            noReferenceChanges: this.conditions.noReferenceChanges(),
            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf(),
            noChanges: this.conditions.noChanges(),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            updateNew: this.updateNew(),
            updatePrev: this.updatePrev(),
            prevRowIsLess: this.rowIsLess("prev_row", "new", [
                "prev_row.id is null",
            ]),
            prevRowIsGreat: this.rowIsGreat("prev_row", "new"),
            selectPrevRowByOrder: this.selectPrevRowByOrder(),
            selectPrevRowByFlag,
            updatePrevRowLastColumnTrue: this.updatePrevRowLastColumnTrue(),
            updateMaxRowLastColumnFalse: this.updateMaxRowLastColumnFalse("prev_row.id"),
            updateThisRowLastColumnTrue: this.updateThisRowLastColumnTrue(),

            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                Expression.unknown(`old.${isLastColumnName}`)
            ]),
            isLastAndSortMinus: Expression.and([
                Expression.unknown(`new.${isLastColumnName}`),
                this.rowIsLess("new", "old")
            ]),
            selectPrevRowWhereGreatOrder,
            isNotLastAndSortPlus: Expression.and([
                Expression.unknown(`not new.${isLastColumnName}`),
                this.rowIsGreat("new", "old")
            ]),
            updatePrevAndThisFlag: this.updatePrevAndThisFlag(
                `(${triggerTable}.id = new.id)`
            ),
            updatePrevAndThisFlagNot: this.updatePrevAndThisFlag(
                `(${triggerTable}.id != new.id)`
            )
        });
        return body;
    }

    private findDataColumns() {
        const dataColumns: string[] = [];
        this.context.cache.select.columns.forEach(selectColumn =>
            selectColumn.expression.getColumnReferences()
                .forEach(columnRef =>{
                    if ( this.context.isColumnRefToTriggerTable(columnRef) ) {
                        if ( !dataColumns.includes(columnRef.name) ) {
                            dataColumns.push(columnRef.name);
                        }
                    }
                })
        );

        return dataColumns;
    }

    private updatePrevAndThisFlag(flagValue: string) {
        const triggerTable = this.context.triggerTable
            .toStringWithoutPublic();

        return new Update({
            table: triggerTable,
            set: [new SetItem({
                column: this.getIsLastColumnName(),
                value: Expression.unknown( flagValue )
            })],
            where: Expression.and([
                `${triggerTable}.id in (new.id, prev_row.id)`
            ])
        });
    }

    private rowIsGreat(greatRow: string, lessRow: string) {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const sortColumnRef = orderBy.expression.getColumnReferences()[0]!;

        return Expression.or([
            Expression.and([
                `${greatRow}.${sortColumnRef.name} is not null`,
                `${lessRow}.${sortColumnRef.name} is null`
            ]),
            `${greatRow}.${sortColumnRef.name} > ${lessRow}.${sortColumnRef.name}`
        ]);
    }

    private rowIsLess(
        lessRow: string,
        greatRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const sortColumnRef = orderBy.expression.getColumnReferences()[0]!;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${lessRow}.${sortColumnRef.name} is null`,
                `${greatRow}.${sortColumnRef.name} is not null`
            ]),
            `${lessRow}.${sortColumnRef.name} < ${greatRow}.${sortColumnRef.name}`
        ]);
    }
}