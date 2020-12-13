import { Expression, IExpressionElement } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class MaxAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = this.columnName === total.toString();
        if ( !totalIsColumn ) {
            return super.plus(total, value);
        }

        return Expression.funcCall("greatest", [
            new Expression([total]),
            value
        ]);
    }
}