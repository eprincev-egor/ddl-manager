import { Update, Expression, SetItem, UnknownExpressionElement, ColumnReference, CaseWhen, SelectColumn, AbstractAstElement } from "../../ast";
import { CoalesceFalseExpression } from "../../ast/expression/CoalesceFalseExpression";
import { TableReference } from "../../database/schema/TableReference";
import { findJoinsMeta, IJoinMeta } from "../processor/findJoinsMeta";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildOneRowBody, ISelectRecord } from "./body/buildOneRowBody";

type Row = "new" | "old";

export class OneRowTriggerBuilder extends AbstractTriggerBuilder {

    createTriggers() {
        return [{
            trigger: this.createDatabaseTrigger(),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }];
    }

    protected createBody() {
        const body = buildOneRowBody({
            selects: this.buildSelectRecords(),
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

    private buildSelectRecords() {
        const selects: ISelectRecord[] = [];
        const joins = findJoinsMeta(this.context.cache.select);
        for (const join of joins) {

            const select: ISelectRecord = {
                recordName: buildRecordName(join),
                select: join.joinedColumns.map(column => column.name),
                from: join.joinedTable.table,
                where: join.joinByColumn.name
            };
            selects.push(select);
        }

        return selects;
    }

    private whereDistinctNewValues() {
        const selectColumns = this.context.cache.select.columns;
        const newValues = selectColumns.map(selectColumn => 
            this.replaceTables(
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
                `${cacheColumnRef} is distinct from (${ values[ i ] })`
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
                value: this.replaceTables(
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
            return this.replaceTables(
                "new", selectColumn.expression
            );
        }

        const {matchedNew} = this.buildMatches();
        return new CaseWhen({
            cases: [{
                when: matchedNew,
                then: this.replaceTables(
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
                this.notColumnToCacheRow(columnRef)
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
                const expression = this.replaceTables(
                    row, selectColumn.expression
                );

                if ( expression.needWrapToBrackets() ) {
                    return `(${expression}) is not null`;
                }
                return `${expression} is not null`;
            });

        if ( this.context.referenceMeta.filters.length ) {
            const filters = this.context.referenceMeta.filters.map(filter =>
                this.replaceTables(row, filter)
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

    private notColumnToCacheRow(columnRef: ColumnReference) {
        return !columnRef.tableReference.equal(
            this.context.cache.for
        );
    }

    private replaceTables(row: Row, expression: Expression) {
        expression = expression.replaceTable(
            this.context.triggerTable,
            new TableReference(
                this.context.triggerTable,
                row
            )
        );

        const joins = findJoinsMeta(this.context.cache.select);
        for (const join of joins) {
            const recordName = buildRecordName(join);

            expression = expression.replaceTable(
                join.joinedTable,
                new TableReference(
                    join.joinedTable.table,
                    recordName
                )
            );
        }

        return expression;
    }
}

function buildRecordName(join: IJoinMeta) {
    const recordName = join.joinedTable.getIdentifier()
        .replace(".", "_") + "_row";
    return recordName;
}