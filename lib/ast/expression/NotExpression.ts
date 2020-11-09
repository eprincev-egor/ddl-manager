import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Expression } from "./Expression";

export class NotExpression extends AbstractExpressionElement {
    readonly not: Expression;

    constructor(not: Expression) {
        super();
        this.not = not;
    }

    template() {
        return [`not(${ this.not })`];
    }

    clone() {
        return new NotExpression(this.not.clone());
    }
}