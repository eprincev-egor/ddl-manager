import { Expression, FuncCall, CaseWhen, IExpressionElement } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class DistinctArrayAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = this.columnName === total.toString();
        if ( !totalIsColumn ) {
            return super.plus(total, value);
        }

        return new Expression([
            new CaseWhen({
                cases: [{
                    when: new Expression([
                        new FuncCall("array_position", [
                            new Expression([total]),
                            value
                        ]),
                        Expression.unknown("is null")
                    ]),
                    then: Expression.funcCall("array_append", [
                        new Expression([total]),
                        value
                    ])
                }],
                else: new Expression([total])
            })
        ]);
    }
}