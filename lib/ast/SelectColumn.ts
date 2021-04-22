import { Database } from "../database/schema/Database";
import { AbstractAstElement } from "./AbstractAstElement";
import { Expression } from "./expression/Expression";
import { Spaces } from "./Spaces";

interface ISelectColumnParams {
    name: string;
    expression: Expression;
}

export class SelectColumn extends AbstractAstElement {
    readonly name!: string;
    readonly expression!: Expression;

    static onlyName(columnName: string) {
        return new SelectColumn({
            name: columnName,
            expression: Expression.unknown(columnName)
        });
    }

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
        const expression = this.expression.toSQL(
            spaces.plusOneLevel()
        );
        if ( expression.trim() === this.name ) {
            return [expression];
        }

        return [`${expression} as ${this.name}`];
    }

    isFuncCall() {
        const funcs = this.expression.getFuncCalls();
        return (
            this.expression.isFuncCall() &&
            funcs[0].name !== "coalesce"
        );
    }

    getAggregations(database: Database) {
        const funcs = this.expression.getFuncCalls();
        const aggFuncs = funcs.filter(funcCall =>
            database.aggregators.includes(funcCall.name)
        );
        return aggFuncs;
    }
}