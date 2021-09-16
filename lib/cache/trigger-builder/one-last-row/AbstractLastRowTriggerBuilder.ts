import {
    Expression, ConditionElementType,
    Update, SetItem,
    Select, SelectColumn,
    ColumnReference, UnknownExpressionElement,
    From, SetSelectItem, Spaces,
    OrderByItem, OrderBy
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
        const orderBy = this.context.cache.select.orderBy!.items[0]!;

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
                                ...this.context.referenceMeta.filters.map(filter =>
                                    filter.replaceTable(
                                        this.fromTable(),
                                        prevRef
                                    )
                                ),
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

        return {
            select,
            for: fromRef
        };
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

    protected updateNew() {
        return new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("new"),
            where: Expression.and([
                this.conditions.simpleWhere("new")!,
                this.whereDistinctRowValues("new")
            ])
        });
    }

    protected updatePrev() {
        return new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("prev_row"),
            where: Expression.and([
                this.conditions.simpleWhere("old")!,
                this.whereDistinctRowValues("prev_row")
            ])
        });
    }

    protected updatePrevRowLastColumnTrue() {
        const triggerTable = this.triggerTableAlias();
        return new Update({
            table: this.fromTable().toString(),
            set: [new SetItem({
                column: this.getIsLastColumnName(),
                value: Expression.unknown("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_row.id`
            ])
        });
    }

    protected updateMaxRowLastColumnFalse(filterBy: string) {
        const isLastColumnName = this.getIsLastColumnName();
        const triggerTable = this.triggerTableAlias();

        return new Update({
            table: this.fromTable().toString(),
            set: [new SetItem({
                column: isLastColumnName,
                value: Expression.unknown("false")
            })],
            where: Expression.and([
                `${triggerTable}.id = ${filterBy}`,
                `${isLastColumnName} = true`
            ])
        });
    }

    protected updateThisRowLastColumn(value: string) {
        const triggerTable = this.triggerTableAlias();

        return new Update({
            table: this.fromTable().toString(),
            set: [new SetItem({
                column: this.getIsLastColumnName(),
                value: Expression.unknown(value)
            })],
            where: Expression.and([
                `${triggerTable}.id = new.id`
            ])
        });
    }

    protected allPrevRowColumns() {
        const selectPrevRowColumnsNames = this.context.triggerTableColumns.slice();
        if  ( !selectPrevRowColumnsNames.includes("id") ) {
            selectPrevRowColumnsNames.unshift("id");
        }

        return selectPrevRowColumnsNames.map(name =>
            SelectColumn.onlyName(name)
        );
    }

    protected selectPrevRowByOrder() {
        return this.context.cache.select.cloneWith({
            columns: this.allPrevRowColumns(),
            where: this.filterTriggerTable("old"),
            intoRow: "prev_row"
        });
    }

    protected filterTriggerTable(
        byRow: string,
        andConditions: ConditionElementType[] = []
    ) {
        const triggerTable = this.triggerTableAlias();

        return Expression.and([
            ...this.context.referenceMeta.columns.map(column =>
                `${triggerTable}.${column} = ${byRow}.${column}`
            ),
            ...this.context.referenceMeta.filters,
            ...andConditions
        ]);
    }

    protected fromTable() {
        const from = this.context.cache.select.from[0]!;
        return from.table;
    }

    protected triggerTableAlias() {
        const fromTable = this.fromTable();
        if ( fromTable.alias ) {
            return fromTable.alias;
        }
        return fromTable.table.toStringWithoutPublic();
    }

    protected findDataColumns() {
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

    protected reselectSetItem() {
        return new SetSelectItem({
            columns: [
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    this.helperColumnName(columnRef.name)
                ),
                ...this.context.cache.select.columns.map(selectColumn =>
                    selectColumn.name
                )
            ],
            select: this.reselect()
                .toSQL( Spaces.level(3) )
                .trim()
        });
    }

    protected reselect() {
        const {select} = this.context.cache;
        const orderByItems = select.orderBy!.items.slice();
        if ( !select.orderBy!.isOnlyId() ) {
            const firstOrder = orderByItems[0]!;
            orderByItems.push(
                new OrderByItem({
                    expression: Expression.unknown(
                        `${this.fromTable().getIdentifier()}.id`
                    ),
                    type: firstOrder.type
                })
            );
        }

        const reselect = select.cloneWith({
            columns: [
                ...this.getOrderByColumnsRefs().map(columnRef =>
                    new SelectColumn({
                        name: this.helperColumnName(columnRef.name),
                        expression: Expression.unknown(
                            this.triggerTableAlias() + "." + columnRef.name
                        )
                    })
                ),
                ...select.columns
            ],
            orderBy: new OrderBy(orderByItems)
        });
        return reselect;
    }

    protected getOrderByColumnsRefs() {
        // TODO: order by hard expression
        const {select} = this.context.cache;
        const orderByColumns = select.orderBy!.getColumnReferences();
        
        const hasId = orderByColumns.some(columnRef => columnRef.name === "id");
        if ( !hasId ) {
            orderByColumns.unshift(
                new ColumnReference(
                    this.fromTable(),
                    "id"
                )
            );
        }

        return orderByColumns;
    }

    protected helperColumnName(triggerTableColumnName: string) {
        return `__${this.context.cache.name}_${triggerTableColumnName}`;
    }

    protected setHelpersByRow(row = "new") {
        const helpers: SetItem[] = this.getOrderByColumnsRefs().map(columnRef =>
            new SetItem({
                column: this.helperColumnName(columnRef.name),
                value: Expression.unknown(row + "." + columnRef.name)
            })
        );
        return helpers;
    }

    protected whereIsGreat(additionalOr: Expression[] = []) {
        const orderBy = this.context.cache.select.orderBy!;

        const cacheTable = (
            this.context.cache.for.alias ||
            this.context.cache.for.table.toStringWithoutPublic()
        );
        const cacheRow = (columnName: string) =>
            `${cacheTable}.${this.helperColumnName(columnName)}`;


        if ( orderBy.isOnlyId() ) {
            if ( orderBy.items[0]!.type === "asc" ) {
                return [
                    `${cacheRow("id")} is null`
                ];
            }
            return [];
        }

        return [orderBy.compareRowsByOrder(
            cacheRow,
            "below",
            "new",
            [
                ...additionalOr,
                `${cacheRow("id")} is null`
            ]
        )];
    }

}