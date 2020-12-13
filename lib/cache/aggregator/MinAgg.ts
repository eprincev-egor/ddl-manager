import { Expression, IExpressionElement } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class MinAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = this.columnName === total.toString();
        if ( !totalIsColumn ) {
            return super.plus(total, value);
        }

        return Expression.funcCall("least", [
            new Expression([total]),
            value
        ]);
    }
}