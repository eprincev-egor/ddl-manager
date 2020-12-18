import { Expression, IExpressionElement, Operator } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class BoolAndAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = this.columnName === total.toString();
        if ( !totalIsColumn ) {
            return super.plus(total, value);
        }

        return new Expression([
            new Expression([total]),
            new Operator("and"),
            value
        ]);
    }
}