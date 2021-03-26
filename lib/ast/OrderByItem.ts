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

        const sortColumnName = this.getFirstColumnRef()!.name;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${leftRow(sortColumnName)} is not distinct from ${rightRow(sortColumnName)}`,
                `${leftRow("id")} ${operator} ${rightRow("id")}`
            ]),
            this.compareRowsNulls(leftRow, vector, rightRow),
            `${leftRow(sortColumnName)} ${operator} ${rightRow(sortColumnName)}`
        ]);
    }

    private compareRowsNulls(
        leftRow: CompareRowFunc,
        vector: "above" | "below",
        rightRow: CompareRowFunc
    ) {
        const sortColumnName = this.getFirstColumnRef()!.name;
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
                `${leftRow(sortColumnName)} is null`,
                `${rightRow(sortColumnName)} is not null`
            ];
        }
        else {
            conditions = [
                `${leftRow(sortColumnName)} is not null`,
                `${rightRow(sortColumnName)} is null`
            ];
        }

        return Expression.and(conditions);
    }
}
