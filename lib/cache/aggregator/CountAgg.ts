import { Expression, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class CountAgg extends AbstractAgg {

    minus() {
        return new Expression([
            this.total,
            new Operator("-"),
            Expression.unknown("1")
        ]);
    }

    plus() {
        return new Expression([
            this.total,
            new Operator("+"),
            Expression.unknown("1")
        ]);
    }

    // istanbul ignore next
    delta(): Expression {
        throw new Error("no matter (+1-1 = 0)");
    }

    default() {
        return "0";
    }
}