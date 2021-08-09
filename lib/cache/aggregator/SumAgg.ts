import { Expression, IExpressionElement, FuncCall, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class SumAgg extends AbstractAgg {

    minus(total: IExpressionElement, value: Expression) {
        return new Expression([
            ...coalesceTotal(total),
            new Operator("-"),
            new FuncCall("coalesce", [
                value,
                Expression.unknown("0")
            ])
        ]);
    }

    plus(total: IExpressionElement, value: Expression) {
        return new Expression([
            ...coalesceTotal(total),
            new Operator("+"),
            new FuncCall("coalesce", [
                value,
                Expression.unknown("0")
            ])
        ]);
    }
}

function coalesceTotal(total: IExpressionElement) {
    const alreadyCoalesced = (
        total instanceof Expression &&
        total.isBinary("-") &&
        total.elements[0] instanceof FuncCall &&
        total.elements[2] instanceof FuncCall &&
        (total.elements[0] as FuncCall).name === "coalesce" &&
        (total.elements[2] as FuncCall).name === "coalesce"
    );
    if ( alreadyCoalesced ) {
        return (total as Expression).elements;
    }

    return [new FuncCall("coalesce", [
        new Expression([total]),
        Expression.unknown("0")
    ])];
}