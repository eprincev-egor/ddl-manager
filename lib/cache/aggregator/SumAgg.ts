import { Expression, FuncCall, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class SumAgg extends AbstractAgg {

    minus(value: Expression) {
        return new Expression([
            this.total,
            new Operator("-"),
            new FuncCall("coalesce", [
                value,
                Expression.unknown("0")
            ])
        ]);
    }

    plus(value: Expression) {
        return new Expression([
            this.total,
            new Operator("+"),
            new FuncCall("coalesce", [
                value,
                Expression.unknown("0")
            ])
        ]);
    }

    default() {
        return "0";
    }
}
