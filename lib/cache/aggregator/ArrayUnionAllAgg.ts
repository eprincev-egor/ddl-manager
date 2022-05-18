import { Expression, IExpressionElement, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

// TODO: this is should be plugin
export class ArrayUnionAllAgg extends AbstractAgg {

    minus(total: IExpressionElement, value: Expression) {
        return Expression.funcCall(
            "cm_array_remove_elements", [
                new Expression([total]),
                value
            ]
        );
    }

    plus(total: IExpressionElement, value: Expression) {
        return new Expression([
            total,
            new Operator("||"),
            value
        ]);
    }
}