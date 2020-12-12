import { FuncCall, Expression, IExpressionElement, SelectColumn } from "../../ast";

export interface IAggParams {
    updateColumn: SelectColumn;
    call: FuncCall;
    total: Expression;
}

export abstract class AbstractAgg {

    readonly call: FuncCall;
    readonly total: Expression;
    protected readonly updateColumn: SelectColumn;

    constructor(params: IAggParams) {
        this.call = params.call;
        this.total = params.total;
        this.updateColumn = params.updateColumn;
    }

    abstract minus(value: Expression): IExpressionElement;
    abstract plus(value: Expression): IExpressionElement;
    abstract delta(prevValue: Expression, nextValue: Expression): IExpressionElement;

    default(): string {
        return "null";
    }
}