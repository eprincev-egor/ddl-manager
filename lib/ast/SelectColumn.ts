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
    recalculateSelect: string;
}

export class SelectColumn {
    readonly name!: string;
    readonly expression!: Expression;
    // TODO: remove it
    readonly recalculateSelect!: string;

    constructor(params: ISelectColumnParams) {
        Object.assign(this, params);
    }

    clone() {
        return new SelectColumn({
            name: this.name,
            expression: this.expression.clone(),
            recalculateSelect: this.recalculateSelect
        });
    }

    replaceExpression(newExpression: Expression) {
        return new SelectColumn({
            name: this.name,
            expression: newExpression,
            recalculateSelect: this.recalculateSelect
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