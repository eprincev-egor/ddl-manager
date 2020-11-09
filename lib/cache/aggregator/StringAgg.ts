import { Expression } from "../../ast";
import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";

export class StringAgg extends AbstractAgg {

    private separator: Expression;
    private arrayAgg: ArrayAgg;

    constructor(params: IAggParams, arrayAgg: ArrayAgg) {
        super(params);
        this.separator = this.call.args[1] as Expression;
        this.arrayAgg = arrayAgg;
    }

    minus(value: Expression) {
        return this.arrayToString(
            this.arrayAgg.minus(value)
        );
    }

    plus(value: Expression) {
        return this.arrayToString(
            this.arrayAgg.plus(value)
        );
    }

    delta(prevValue: Expression, nextValue: Expression) {
        return this.arrayToString(
            this.arrayAgg.delta(prevValue, nextValue)
        );
    }

    private arrayToString(arrayAgg: Expression) {
        return Expression.funcCall(
            this.arrayToStringFuncName(), [
                arrayAgg,
                this.separator
            ]
        );
    }

    private arrayToStringFuncName() {
        if ( this.call.distinct ) {
            return "cm_array_to_string_distinct";
        }
        else {
            return "array_to_string";
        }
    }
}