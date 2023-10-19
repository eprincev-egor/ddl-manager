import { Database } from "../database/schema/Database";
import { TableID } from "../database/schema/TableID";
import { TableReference } from "../database/schema/TableReference";
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

    clone(params: Partial<ISelectColumnParams> = {}) {
        return new SelectColumn({
            name: this.name,
            expression: this.expression.clone(),
            ...params
        });
    }

    replaceExpression(newExpression: Expression) {
        return new SelectColumn({
            name: this.name,
            expression: newExpression
        });
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new SelectColumn({
            name: this.name,
            expression: this.expression
                .replaceTable(replaceTable, toTable)
        });
    }

    template(spaces: Spaces) {
        const expression = this.expression.toSQL(
            spaces.plusOneLevel()
        );
        if ( expression.trim() === this.name || this.name === "*" ) {
            return [expression];
        }

        return [`${expression} as ${this.name}`];
    }

    isAggCall(database: Database) {
        const funcs = this.expression.getFuncCalls();
        return (
            this.expression.isFuncCall() &&
            database.aggregators.includes( funcs[0].name )
        );
    }

    getAggregations(aggFunctions: string[]) {
        const funcs = this.expression.getFuncCalls();
        const aggFuncs = funcs.filter(funcCall =>
            aggFunctions.includes(funcCall.name)
        );
        return aggFuncs;
    }
}