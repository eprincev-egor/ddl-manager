import {
    Expression,
    Update, SetItem, 
    SelectColumn
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByMutableBody } from "../body/buildOneLastRowByMutableBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    protected createBody() {
        const {select} = this.context.cache;
        const orderBy = select.orderBy!;
        const sortColumnRef = orderBy!.getColumnReferences()[0]!;

        const triggerTable = this.triggerTableAlias();
        const isLastColumnName = this.getIsLastColumnName();

        const selectPrevRowByFlag = select.cloneWith({
            columns: [
                SelectColumn.onlyName("id"),
                SelectColumn.onlyName(sortColumnRef.name)
            ],
            where: this.filterTriggerTable("new", [
                `${triggerTable}.${isLastColumnName} = true`
            ]),
            orderBy: undefined,
            limit: undefined,
            intoRow: "prev_row"
        });

        const selectPrevRowWhereGreatOrder = select.cloneWith({
            columns: this.allPrevRowColumns(),
            where: this.filterTriggerTable("new", [
                orderBy.compareRowsByOrder(triggerTable, ">", "new"),
                `${triggerTable}.id <> new.id`
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
            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf()!,
            noChanges: this.conditions.noChanges(),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            updateNew: this.updateNew(),
            updatePrev: this.updatePrev(),
            prevRowIsLess: orderBy.compareRowsByOrder("prev_row", "<", "new", [
                "prev_row.id is null",
            ]),
            prevRowIsGreat: orderBy.compareRowsByOrder("prev_row", ">", "new"),
            selectPrevRowByOrder: this.selectPrevRowByOrder(),
            selectPrevRowByFlag,

            updatePrevRowLastColumnTrue: this.updatePrevRowLastColumnTrue(),
            updateMaxRowLastColumnFalse: this.updateMaxRowLastColumnFalse("prev_row.id"),

            updateThisRowLastColumnFalse: this.updateThisRowLastColumn("false"),
            updateThisRowLastColumnTrue: this.updateThisRowLastColumn("true"),

            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                Expression.unknown(`old.${isLastColumnName}`)
            ]),
            isLastAndSortMinus: Expression.and([
                Expression.unknown(`new.${isLastColumnName}`),
                orderBy.compareRowsByOrder("new", "<", "old")
            ]),
            selectPrevRowWhereGreatOrder,
            isNotLastAndSortPlus: Expression.and([
                Expression.unknown(`not new.${isLastColumnName}`),
                orderBy.compareRowsByOrder("new", ">", "old")
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

    private updatePrevAndThisFlag(flagValue: string) {
        const triggerTable = this.triggerTableAlias();

        return new Update({
            table: this.fromTable().toString(),
            set: [new SetItem({
                column: this.getIsLastColumnName(),
                value: Expression.unknown( flagValue )
            })],
            where: Expression.and([
                `${triggerTable}.id in (new.id, prev_row.id)`
            ])
        });
    }
}