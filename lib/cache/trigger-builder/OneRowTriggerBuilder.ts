import { Update, Expression, SetItem, UnknownExpressionElement, ColumnReference, CaseWhen, SelectColumn, AbstractAstElement } from "../../ast";
import { CoalesceFalseExpression } from "../../ast/expression/CoalesceFalseExpression";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildOneRowBody } from "./body/buildOneRowBody";

type Row = "new" | "old";

export class OneRowTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const body = buildOneRowBody({
            onInsert: this.context.withoutInsertCase() ?  undefined : {
                needUpdate: this.hasEffect("new"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setNewItems(),
                    where: Expression.and([
                        this.conditions.simpleWhere("new")!,
                        this.whereDistinctNewValues()
                    ])
                })
            },
            onUpdate: {
                needUpdate: this.needUpdateOnUpdate(),
                noChanges: this.conditions.noChanges(),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setDeltaItems(),
                    where: Expression.and([
                        this.conditions.simpleWhere("new")!,
                        this.whereDistinctDeltaValues()
                    ])
                })
            },
            onDelete: {
                needUpdate: this.hasEffect("old"),
                update: new Update({
                    table: this.context.cache.for.toString(),
                    set: this.setNulls(),
                    where: Expression.and([
                        this.conditions.simpleWhere("old")!,
                        this.whereDistinctNullValues()
                    ])
                })
            }
        });
        return body;
    }

    private whereDistinctNewValues() {
        const selectColumns = this.context.cache.select.columns;
        const newValues = selectColumns.map(selectColumn => 
            this.replaceTriggerTableToRow(
                "new", selectColumn.expression
            )
        );
        return this.whereDistinctFrom(newValues);
    }

    private whereDistinctDeltaValues() {
        const selectColumns = this.context.cache.select.columns;
        const newValues = selectColumns.map(selectColumn => 
            this.caseMatchedThenNewValue(selectColumn)
        );
        return this.whereDistinctFrom(newValues);
    }

    private whereDistinctNullValues() {
        const selectColumns = this.context.cache.select.columns;
        const nullValues = selectColumns.map(selectColumn => 
            this.createNullExpression(selectColumn.expression)
        );
        return this.whereDistinctFrom(nullValues);
    }

    private whereDistinctFrom(values: AbstractAstElement[]) {
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

    private needUpdateOnUpdate() {
        if ( !this.context.referenceMeta.filters.length ) {
            return;
        }

        const {matchedNew, matchedOld} = this.buildMatches();
        return Expression.or([
            matchedOld,
            matchedNew
        ]);
    }

    private buildMatches() {
        const matchedOld: CoalesceFalseExpression[] = [];
        const matchedNew: CoalesceFalseExpression[] = [];

        this.context.referenceMeta.filters.forEach(filter => {
            const filterOld = this.replaceTriggerTableToRow("old", filter);
            const filterNew = this.replaceTriggerTableToRow("new", filter);

            matchedNew.push(
                new CoalesceFalseExpression(filterNew)
            );
            matchedOld.push(
                new CoalesceFalseExpression(filterOld)
            );
        });

        return {
            matchedOld: Expression.and(matchedOld),
            matchedNew: Expression.and(matchedNew)
        };
    }

    private setNewItems() {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: this.replaceTriggerTableToRow(
                    "new", selectColumn.expression
                )
            })
            return setItem;
        });
        return setItems;
    }

    private setDeltaItems() {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: this.caseMatchedThenNewValue(
                    selectColumn
                )
            })
            return setItem;
        });
        return setItems;
    }

    private caseMatchedThenNewValue(
        selectColumn: SelectColumn
    ) {
        if ( !this.context.referenceMeta.filters.length ) {
            return this.replaceTriggerTableToRow(
                "new", selectColumn.expression
            );
        }

        const {matchedNew} = this.buildMatches();
        return new CaseWhen({
            cases: [{
                when: matchedNew,
                then: this.replaceTriggerTableToRow(
                    "new", selectColumn.expression
                )
            }],
            else: Expression.unknown("null")
        });
    }

    private setNulls() {
        const setItems = this.context.cache.select.columns.map(selectColumn => {
            const setItem = new SetItem({
                column: selectColumn.name,
                value: this.createNullExpression(
                    selectColumn.expression
                )
            })
            return setItem;
        });
        return setItems;
    }

    private createNullExpression(expression: Expression) {

        if (
            expression.isColumnReference() ||
            expression.isArrayItemOfColumnReference()
        ) {
            return Expression.unknown("null");
        }

        let nullExpression = expression;
        const columnRefs = expression.getColumnReferences()
            .filter(columnRef =>
                this.columnRefToTriggerTable(columnRef)
            );

        for (const columnRef of columnRefs) {
            const dbTable = this.context.database.getTable(
                columnRef.tableReference.table
            );
            const dbColumn = dbTable && dbTable.getColumn( columnRef.name );
            
            const nullSql = dbColumn ? 
                UnknownExpressionElement.fromSql(`(null::${ dbColumn.type })`) :
                UnknownExpressionElement.fromSql("null");

            nullExpression = nullExpression.replaceColumn(columnRef, nullSql);
        }

        return nullExpression;
    }

    private hasEffect(row: Row) {
        const conditions = this.context.cache.select.columns
            .filter(selectColumn =>
                selectColumn.expression.getColumnReferences()
                    .every(columnRef =>
                        this.columnRefToTriggerTable(columnRef)
                    )
            )
            .map(selectColumn => {
                const expression = this.replaceTriggerTableToRow(
                    row, selectColumn.expression
                );
                return expression.toString() + " is not null";
            });

        if ( this.context.referenceMeta.filters.length ) {
            const filters = this.context.referenceMeta.filters.map(filter =>
                this.replaceTriggerTableToRow(row, filter)
            );

            if ( conditions.length ) {
                const hasEffectAndFilters = Expression.and([
                    Expression.or(conditions),
                    Expression.and(filters)
                ]);
                return hasEffectAndFilters;
            }

            return Expression.and(filters);
        }

        return Expression.or(conditions);
    }

    private columnRefToTriggerTable(columnRef: ColumnReference) {
        return columnRef.tableReference.equal(
            this.context.cache.select.from[0]!.table
        );
    }
}