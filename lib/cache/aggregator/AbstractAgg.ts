import { FuncCall, Expression, SelectColumn, IExpressionElement } from "../../ast";

export interface IAggParams {
    updateColumn: SelectColumn;
    call: FuncCall;
    total: IExpressionElement;
}

export abstract class AbstractAgg {

    readonly call: FuncCall;
    readonly total: IExpressionElement;
    readonly helpersAgg?: AbstractAgg[];

    constructor(params: IAggParams) {
        this.call = params.call;
        this.total = params.total;
    }

    abstract minus(value: Expression): Expression;
    abstract plus(value: Expression): Expression;

    default(): string {
        return "null";
    }
}