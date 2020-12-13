import { Expression, IExpressionElement, Operator } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class StringAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = /^\w+$/.test(total.toString().trim())

        const cannotOptimize = (
            this.call.distinct ||
            this.call.orderBy.length ||
            !totalIsColumn
        );
        if ( cannotOptimize ) {
            return super.plus(total, value);
        }

        const separator = this.call.args[1] as Expression;

        return Expression.funcCall("coalesce", [
            new Expression([
                total,
                new Operator("||"),
                Expression.funcCall("coalesce", [
                    new Expression([
                        separator,
                        new Operator("||"),
                        value
                    ]),
                    Expression.unknown("''")
                ])
            ]),
            value
        ])
    }

}