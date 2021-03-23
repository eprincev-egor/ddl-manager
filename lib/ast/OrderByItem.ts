import { AbstractAstElement } from "./AbstractAstElement";
import { Expression } from "./expression";
import { Spaces } from "./Spaces";

export type OrderItemType = "asc" | "desc";
export type OrderItemNulls = "first" | "last";
export interface OrderByItemParams {
    expression: Expression;
    type?: OrderItemType;
    nulls?: OrderItemNulls;
}

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
}