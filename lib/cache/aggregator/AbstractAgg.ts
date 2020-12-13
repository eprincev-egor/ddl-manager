import { FuncCall, Expression, IExpressionElement } from "../../ast";

export interface IAggParams {
    call: FuncCall;
    columnName: string;
}

export abstract class AbstractAgg {

    readonly call: FuncCall;
    readonly columnName: string;
    readonly helpersAgg?: AbstractAgg[];

    constructor(params: IAggParams) {
        this.call = params.call;
        this.columnName = params.columnName;
    }

    abstract minus(total: IExpressionElement, value: Expression): Expression;
    abstract plus(total: IExpressionElement, value: Expression): Expression;

    default(): string {
        return "null";
    }
}