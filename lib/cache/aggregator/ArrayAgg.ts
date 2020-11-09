import { Expression } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class ArrayAgg extends AbstractAgg {

    minus(value: Expression) {
        return Expression.funcCall(
            "cm_array_remove_one_element", [
                this.total,
                value
            ]
        );
    }

    plus(value: Expression) {
        return Expression.funcCall(
            "array_append", [
                this.total,
                value
            ]
        );
    }

    delta(prevValue: Expression, nextValue: Expression) {
        return Expression.funcCall(
            "array_append", [
                Expression.funcCall(
                    "cm_array_remove_one_element", [
                        this.total,
                        prevValue
                    ]
                ),
                nextValue
            ]
        );
    }
}