import {
    Expression,
    Update, SetItem, 
    SelectColumn
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByMutableBody } from "../body/buildOneLastRowByMutableBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    protected createBody() {
        const orderBy = this.context.cache.select.orderBy[0]!;
        const sortColumnRef = orderBy.expression.getColumnReferences()[0]!;
        const prevRowIsLess = Expression.or([
            "prev_row.id is null",
            Expression.and([
                `prev_row.${sortColumnRef.name} is null`,
                `new.${sortColumnRef.name} is not null`
            ]),
            `prev_row.${sortColumnRef.name} < new.${sortColumnRef.name}`
        ]);
        const prevRowIsGreat = Expression.or([
            Expression.and([
                `prev_row.${sortColumnRef.name} is not null`,
                `new.${sortColumnRef.name} is null`
            ]),
            Expression.unknown(
                `prev_row.${sortColumnRef.name} > new.${sortColumnRef.name}`
            )
        ]);

        const triggerTable = this.context.triggerTable.toStringWithoutPublic();
        const isLastColumnName = this.getIsLastColumnName();

        const updatePrevAndThisFlag = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: Expression.unknown(
                    `(${triggerTable}.id = new.id)`
                )
            })],
            where: Expression.and([
                `${triggerTable}.id in (new.id, prev_row.id)`
            ])
        });

        const updatePrevAndThisFlagNot = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: Expression.unknown(
                    `(${triggerTable}.id != new.id)`
                )
            })],
            where: Expression.and([
                `${triggerTable}.id in (new.id, prev_row.id)`
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
        });

        const selectPrevRowWhereGreatOrder = this.context.cache.select.cloneWith({
            columns: selectPrevRowColumns,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                ...this.context.referenceMeta.filters,
                // TODO: check alias
                Expression.unknown(
                    `${triggerTable}.${sortColumnRef.name} > new.${sortColumnRef.name}`
                )
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
            prevRowIsLess,
            prevRowIsGreat,
            updatePrevAndThisFlag,
            selectPrevRowByOrder,
            selectPrevRowByFlag,
            updatePrevRowLastColumnTrue: this.updatePrevRowLastColumnTrue(),
            ifNeedUpdateNewOnChangeReference: Expression.or([
                "prev_row.id is null",
                Expression.and([
                    `prev_row.${sortColumnRef.name} is null`,
                    `new.${sortColumnRef.name} is not null`
                ]),
                `prev_row.${sortColumnRef.name} < new.${sortColumnRef.name}`
            ]),
            updateMaxRowLastColumnFalse: this.updateMaxRowLastColumnFalse("prev_row.id"),
            updateThisRowLastColumnTrue: this.updateThisRowLastColumnTrue(),

            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                Expression.unknown(`old.${isLastColumnName}`)
            ]),
            isLastAndSortMinus: Expression.and([
                Expression.unknown(`new.${isLastColumnName}`),
                Expression.or([
                    Expression.and([
                        `new.${sortColumnRef.name} is null`,
                        `old.${sortColumnRef.name} is not null`
                    ]),
                    `new.${sortColumnRef.name} < old.${sortColumnRef.name}`
                ])
            ]),
            selectPrevRowWhereGreatOrder,
            isNotLastAndSortPlus: Expression.and([
                Expression.unknown(`not new.${isLastColumnName}`),
                Expression.or([
                    Expression.and([
                        `new.${sortColumnRef.name} is not null`,
                        `old.${sortColumnRef.name} is null`
                    ]),
                    `new.${sortColumnRef.name} > old.${sortColumnRef.name}`
                ])
            ]),
            updatePrevAndThisFlagNot
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
}