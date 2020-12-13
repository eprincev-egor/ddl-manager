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

    default() {
        return "0";
    }
}