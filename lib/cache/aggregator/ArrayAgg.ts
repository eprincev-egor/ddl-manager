import { Expression, IExpressionElement } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class ArrayAgg extends AbstractAgg {

    minus(total: IExpressionElement, value: Expression) {
        return Expression.funcCall(
            "cm_array_remove_one_element", [
                new Expression([total]),
                value
            ]
        );
    }

    plus(total: IExpressionElement, value: Expression) {
        return Expression.funcCall(
            this.chooseAppendFunction(), [
                new Expression([total]),
                value
            ]
        );
    }

    private chooseAppendFunction() {
        if ( this.call.orderBy && this.call.orderBy.items.length === 1 ) {
            const orderByItem = this.call.orderBy.items[0];

            if ( orderByItem.type === "asc" ) {
                if ( orderByItem.nulls === "last" ) {
                    return "cm_array_append_order_by_asc_nulls_last";
                }

                if ( orderByItem.nulls === "first" ) {
                    return "cm_array_append_order_by_asc_nulls_first";
                }
            }

            if ( orderByItem.type === "desc" ) {
                if ( orderByItem.nulls === "first" ) {
                    return "cm_array_append_order_by_desc_nulls_first";
                }

                if ( orderByItem.nulls === "last" ) {
                    return "cm_array_append_order_by_desc_nulls_last";
                }
            }
        }

        return "array_append";
    }
}