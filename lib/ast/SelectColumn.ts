import { AbstractAstElement } from "./AbstractAstElement";
import { Expression } from "./expression/Expression";
import { Spaces } from "./Spaces";

// TODO: load from db
const aggFuncsNames = [
    "count",
    "max",
    "min",
    "sum",
    "array_agg",
    "string_agg"
];

interface ISelectColumnParams {
    name: string;
    expression: Expression;
}

export class SelectColumn extends AbstractAstElement {
    readonly name!: string;
    readonly expression!: Expression;

    constructor(params: ISelectColumnParams) {
        super();
        Object.assign(this, params);
    }

    clone() {
        return new SelectColumn({
            name: this.name,
            expression: this.expression.clone()
        });
    }

    replaceExpression(newExpression: Expression) {
        return new SelectColumn({
            name: this.name,
            expression: newExpression
        });
    }

    template(spaces: Spaces) {
        return [`${this.expression.toSQL(spaces.plusOneLevel() )} as ${this.name}`];
    }

    getAggregations() {
        const funcs = this.expression.getFuncCalls();
        const aggFuncs = funcs.filter(funcCall =>
            aggFuncsNames.includes(funcCall.name)
        );
        return aggFuncs;
    }
}