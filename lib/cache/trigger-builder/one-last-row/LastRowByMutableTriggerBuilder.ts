import { ColumnReference, Expression, From, Select, SelectColumn, Update } from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByMutableBody } from "../body/buildOneLastRowByMutableBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    createTriggers() {
        return [{
            trigger: this.createDatabaseTriggerOnDIU(),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }];
    }

    protected createBody() {
        const {select} = this.context.cache;
        const orderBy = select.orderBy!;
        const sortColumnRef = orderBy!.getColumnReferences()[0]!;

        const cacheTable = (
            this.context.cache.for.alias ||
            this.context.cache.for.table.toStringWithoutPublic()
        );
        const lastIdColumnName = this.helperColumnName("id");

        const body = buildOneLastRowByMutableBody({
            noChanges: this.conditions.noChanges(),
            noReferenceChanges: this.conditions.noReferenceChanges(),
            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf()!,
            newSortIsGreat: orderBy.compareRowsByOrder("new", "above", "old"),
            hasSortChanges: Expression.and([
                `new.${sortColumnRef.name} is distinct from old.${sortColumnRef.name}`
            ]),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,

                updateReselectCacheRow: new Update({
                table: this.context.cache.for.toString(),
                set: [this.reselectSetItem()],
                where: Expression.and([
                    `${cacheTable}.id = cache_row.id`
                ])
            }),
            oldIsLast: Expression.and([`cache_row.${lastIdColumnName} = old.id`]),
            newIsLast: Expression.and([`cache_row.${lastIdColumnName} = new.id`]),
            newSortIsLessThenCacheRow: orderBy.compareRowsByOrder(
                "new", "above",
                (columnName) => `cache_row.${this.helperColumnName(columnName)}`
            ),
            selectByOldAndLockCacheRow: this.selectAndLock("old"),
            selectByNewAndLockCacheRow: this.selectAndLock("new"),
            updateNew: new Update({
                table: this.context.cache.for.toString(),
                set: [
                    ...this.setHelpersByRow(),
                    ...this.setItemsByRow("new")
                ],
                where: this.conditions.simpleWhere("new")!
            }),
            updateNewWhereIsGreat: new Update({
                table: this.context.cache.for.toString(),
                set: [
                    ...this.setHelpersByRow(),
                    ...this.setItemsByRow("new")
                ],
                where: Expression.and([
                    this.conditions.simpleWhere("new")!,
                    Expression.and(this.whereIsGreat())
                ])
            }),
            updateNewWhereIsLastAndDistinctNew: new Update({
                table: this.context.cache.for.toString(),
                set: this.setItemsByRow("new"),
                where: Expression.and([
                    this.conditions.simpleWhere("new")!,
                    `${cacheTable}.${ lastIdColumnName } = new.id`,
                    this.whereDistinctRowValues("new")
                ])
            })
        });
        return body;
    }

    private selectAndLock(row: "new" | "old") {
        return new Select({
            columns: [
                new SelectColumn({
                    name: "id",
                    expression: new Expression([
                        new ColumnReference(
                            this.context.cache.for, "id"
                        )
                    ])
                }),
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    new SelectColumn({
                        name: this.helperColumnName(columnRef.name),
                        expression: new Expression([
                            new ColumnReference(
                                this.context.cache.for,
                                this.helperColumnName(columnRef.name)
                            )
                        ])
                    })
                )
            ],
            from: [new From(this.context.cache.for)],
            where: this.conditions.simpleWhere(row)!,
            forUpdate: true,
            intoRow: "cache_row"
        });
    }
}