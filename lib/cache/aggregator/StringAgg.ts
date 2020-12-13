import { Expression, CaseWhen, IExpressionElement, Operator, FuncCall } from "../../ast";
import { UniversalAgg } from "./UniversalAgg";

export class StringAgg extends UniversalAgg {

    plus(total: IExpressionElement, value: Expression) {
        const totalIsColumn = this.columnName === total.toString();

        const cannotOptimize = (
            this.call.orderBy.length ||
            !totalIsColumn
        );
        if ( cannotOptimize ) {
            return super.plus(total, value);
        }

        if ( this.call.distinct ) {
            return this.plusDistinctString(total, value);
        }
        else {
            return this.plusString(total, value);
        }
    }

    private plusDistinctString(total: IExpressionElement, value: Expression) {
        const firstArg = this.call.args[0] as Expression;
        const canOptimize = /^[\w\.]+$/.test(firstArg.toString())
        if ( !canOptimize ) {
            return super.plus(total, value);
        }

        return new Expression([
            new CaseWhen({
                cases: [{
                    when: new Expression([
                        new FuncCall("array_position", [
                            Expression.unknown(this.helpersAgg[0].columnName),
                            value
                        ]),
                        Expression.unknown("is null")
                    ]),
                    then: this.plusString(total, value)
                }],
                else: new Expression([total])
            })
        ]);
    }

    private plusString(total: IExpressionElement, value: Expression) {
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
        ]);
    }
}