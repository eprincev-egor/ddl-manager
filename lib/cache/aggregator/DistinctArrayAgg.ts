import { Expression } from "../../ast";
import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";

export class DistinctArrayAgg extends AbstractAgg {

    private arrayAgg: ArrayAgg;

    constructor(params: IAggParams, arrayAgg: ArrayAgg) {
        super(params);
        this.arrayAgg = arrayAgg;
    }

    minus(value: Expression) {
        return this.distinctArray(
            this.arrayAgg.minus(value)
        );
    }

    plus(value: Expression) {
        return this.distinctArray(
            this.arrayAgg.plus(value)
        );
    }

    delta(prevValue: Expression, nextValue: Expression) {
        return this.distinctArray(
            this.arrayAgg.delta(prevValue, nextValue)
        );
    }

    private distinctArray(arrayAgg: Expression) {
        return Expression.funcCall(
            "cm_distinct_array", [
                arrayAgg
            ]
        );
    }

}