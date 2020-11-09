import { CaseWhen, Expression, Operator } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";

export class MaxAgg extends AbstractAgg {

    minus(value: Expression) {
        return new CaseWhen({
            cases: [
                {
                    when: new Expression([
                        this.total,
                        new Operator(">"),
                        value
                    ]),
                    then: this.total
                }
            ],
            else: Expression.unknown([
                "(",
                ...this.printSelect("    ")
                    .split("\n"),
                ")"
            ].join("\n"))
        });
    }

    plus(value: Expression) {
        return Expression.funcCall("greatest", [
            this.total,
            value
        ]);
    }

    delta(prevValue: Expression, nextValue: Expression) {
        return new CaseWhen({
            cases: [
                {
                    when: new Expression([
                        nextValue,
                        new Operator(">"),
                        this.total
                    ]),
                    then: nextValue
                },
                {
                    when: new Expression([
                        prevValue,
                        new Operator("<"),
                        this.total
                    ]),
                    then: this.total
                }
            ],
            else: Expression.unknown([
                "(",
                ...this.printSelect("    ")
                    .split("\n"),
                ")"
            ].join("\n"))
        });
    }
}
