import { Expression, IExpressionElement, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class CountAgg extends AbstractAgg {

    minus(total: IExpressionElement) {
        return new Expression([
            total,
            new Operator("-"),
            Expression.unknown("1")
        ]);
    }

    plus(total: IExpressionElement) {
        return new Expression([
            total,
            new Operator("+"),
            Expression.unknown("1")
        ]);
    }

    default() {
        return "0";
    }
}