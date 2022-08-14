import { TableID } from "../database/schema/TableID";
import { TableReference } from "../database/schema/TableReference";
import { AbstractAstElement } from "./AbstractAstElement";
import { ConditionElementType, Expression } from "./expression";
import { Spaces } from "./Spaces";

export type OrderItemType = "asc" | "desc";
export type OrderItemNulls = "first" | "last";
export interface OrderByItemParams {
    expression: Expression;
    type?: OrderItemType;
    nulls?: OrderItemNulls;
}
export type CompareRowFunc = ((columnName: string) => string);
export type CompareRow = (
    string |
    CompareRowFunc
);

export class OrderByItem extends AbstractAstElement {

    readonly expression: Expression;
    readonly type: OrderItemType;
    readonly nulls: OrderItemNulls;
    constructor(params: OrderByItemParams) {
        super();
        this.expression = params.expression;
        this.type = params.type || "asc";

        // https://postgrespro.ru/docs/postgrespro/10/queries-order
        // По умолчанию значения NULL считаются больше любых других, 
        this.nulls = (
            params.nulls || (
                // то есть подразумевается NULLS FIRST для порядка DESC и 
                this.type === "desc" ? 
                    "first" :
                // NULLS LAST в противном случае.
                    "last"
            )
        )
    }

    clone() {
        return new OrderByItem({
            expression: this.expression.clone(),
            type: this.type,
            nulls: this.nulls
        });
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new OrderByItem({
            expression: this.expression
                .replaceTable(replaceTable, toTable),
            type: this.type,
            nulls: this.nulls
        });
    }

    equal(item: OrderByItem) {
        return (
            this.expression.equal(item.expression) &&
            this.type == item.type &&
            this.nulls == item.nulls
        );
    }

    getColumnReferences() {
        return this.expression.getColumnReferences();
    }

    template(spaces: Spaces): string[] {
        return [
            this.expression.toSQL( spaces ) +
                    " " + this.type +
                    " nulls " + this.nulls
        ];
    }

    getFirstColumnRef() {
        return this.getColumnReferences()[0];
    }

    compareRowsByOrder(
        leftRow: CompareRow,
        vector: "above" | "below",
        rightRow: CompareRow,
        orPreConditions: ConditionElementType[]
    ) {
        const operator = (
            this.type === "asc" && vector === "above" ||
            this.type === "desc" && vector === "below"
        ) ? "<" : ">";

        if ( typeof leftRow === "string" ) {
            const rowName = leftRow;
            leftRow = (columnName: string) => `${rowName}.${columnName}`;
        }
        if ( typeof rightRow === "string" ) {
            const rowName = rightRow;
            rightRow = (columnName: string) => `${rowName}.${columnName}`;
        }

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${this.printRowValue(leftRow)} is not distinct from ${this.printRowValue(rightRow)}`,
                `${leftRow("id")} ${operator} ${rightRow("id")}`
            ]),
            this.compareRowsNulls(leftRow, vector, rightRow),
            `${this.printRowValue(leftRow)} ${operator} ${this.printRowValue(rightRow)}`
        ]);
    }

    private compareRowsNulls(
        leftRow: CompareRowFunc,
        vector: "above" | "below",
        rightRow: CompareRowFunc
    ) {
        let conditions: string[] = [];

        // https://postgrespro.ru/docs/postgrespro/10/queries-order
        // По умолчанию значения NULL считаются больше любых других, 
        const leftShouldBeNull = (
            vector === "above" && this.nulls === "first"
            ||
            vector === "below" && this.nulls === "last"
        );

        if ( leftShouldBeNull ) {
            conditions = [
                `${this.printRowValue(leftRow)} is null`,
                `${this.printRowValue(rightRow)} is not null`
            ];
        }
        else {
            conditions = [
                `${this.printRowValue(leftRow)} is not null`,
                `${this.printRowValue(rightRow)} is null`
            ];
        }

        return Expression.and(conditions);
    }

    private printRowValue(row: CompareRowFunc) {
        let expression = this.expression;
        for (const columnRef of this.getColumnReferences()) {
            expression = expression.replaceColumn(
                columnRef,
                Expression.unknown(row(columnRef.name))
            );
        }
        return expression.toString();
    }
}
