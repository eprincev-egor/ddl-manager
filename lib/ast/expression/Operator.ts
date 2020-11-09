import { AbstractExpressionElement } from "./AbstractExpressionElement";

export class Operator extends AbstractExpressionElement {
    private readonly operator: string;
    constructor(operator: string) {
        super();
        this.operator = operator;
    }

    clone() {
        return new Operator(this.operator);
    }

    template() {
        return [this.operator];
    }
}