import { Update, Expression, SetItem, UnknownExpressionElement, SelectColumn, Select, From } from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByMutableBody } from "../body/buildOneLastRowByMutableBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    protected createBody() {
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

        const orderBy = this.context.cache.select.orderBy[0]!;
        const sortColumnRef = orderBy.expression.getColumnReferences()[0]!;
        const prevRowIsLess = Expression.or([
            "prev_row.id is null",
            `prev_row.${sortColumnRef.name} < new.${sortColumnRef.name}`
        ]);

        const triggerTable = this.context.triggerTable.toStringWithoutPublic();
        const isLastColumnName = this.getIsLastColumnName();

        const updatePrevAndThisFlag = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql(
                    `(${triggerTable}.id = new.id)`
                )
            })],
            where: Expression.and([
                `${triggerTable}.id in (new.id, prev_row.id)`
            ])
        });

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

        const selectPrevRowByOrder = this.context.cache.select.cloneWith({
            columns: selectPrevRowColumns,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = old.${column}`
                ),
                ...this.context.referenceMeta.filters
            ]),
            intoRow: "prev_row"
        });

        const selectPrevRowByFlag = this.context.cache.select.cloneWith({
            columns: [
                new SelectColumn({
                    name: "id",
                    expression: Expression.unknown("id")
                }),
                new SelectColumn({
                    name: sortColumnRef.name,
                    expression: Expression.unknown(sortColumnRef.name)
                })
            ],
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                ...this.context.referenceMeta.filters,
                // TODO: check alias
                Expression.unknown(`${triggerTable}.${isLastColumnName} = true`)
            ]),
            orderBy: [],
            limit: undefined,
            intoRow: "prev_row"
        })

        const body = buildOneLastRowByMutableBody({
            isLastColumn: isLastColumnName,
            noReferenceAndSortChanges: Expression.and([
                this.conditions.noReferenceChanges(),
                `new.${sortColumnRef.name} is not distinct from old.${sortColumnRef.name}`
            ]),
            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf(),
            noChanges: this.conditions.noChanges(),
            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            updateNew,
            updatePrev,
            prevRowIsLess,
            updatePrevAndThisFlag,
            selectPrevRowByOrder,
            selectPrevRowByFlag,
            updatePrevRowLastColumnTrue
        });
        return body;
    }

}