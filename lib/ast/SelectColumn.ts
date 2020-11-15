import { Expression } from "./expression/Expression";

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

export class SelectColumn {
    readonly name!: string;
    readonly expression!: Expression;

    constructor(params: ISelectColumnParams) {
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

    toString() {
        return `${this.expression} as ${this.name}`;
    }

    getAggregations() {
        const funcs = this.expression.getFuncCalls();
        const aggFuncs = funcs.filter(funcCall =>
            aggFuncsNames.includes(funcCall.name)
        );
        return aggFuncs;
    }
}