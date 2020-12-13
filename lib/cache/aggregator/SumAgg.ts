import { Expression, IExpressionElement, FuncCall, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class SumAgg extends AbstractAgg {

    minus(total: IExpressionElement, value: Expression) {
        return new Expression([
            ...this.extrudeExpression(total),
            new Operator("-"),
            new FuncCall("coalesce", [
                value,
                Expression.unknown("0")
            ])
        ]);
    }

    plus(total: IExpressionElement, value: Expression) {
        return new Expression([
            ...this.extrudeExpression(total),
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
    
    // fix delta case:
    // column = (column - old) + new
    // =>
    // column = column - old + new
    private extrudeExpression(total: IExpressionElement) {
        if ( total instanceof Expression ) {
            return total.elements;
        }
        return [total];
    }
}
