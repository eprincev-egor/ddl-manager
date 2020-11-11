import { FuncCall, Expression, IExpressionElement } from "../../ast";

export interface IAggParams {
    call: FuncCall;
    total: Expression;
    recalculateSelect: string;
}

export abstract class AbstractAgg {

    readonly call: FuncCall;
    protected readonly total: Expression;
    protected readonly recalculateSelect: string;

    constructor(params: IAggParams) {
        this.call = params.call;
        this.total = params.total;
        this.recalculateSelect = params.recalculateSelect;
    }

    abstract minus(value: Expression): IExpressionElement;
    abstract plus(value: Expression): IExpressionElement;
    abstract delta(prevValue: Expression, nextValue: Expression): IExpressionElement;

    default(): string {
        return "null";
    }

    protected printSelect(spaces: string) {
        const selectSQL = spaces + this.recalculateSelect.replace(/\n/g, `\n${spaces}`);
        return selectSQL;
    }
}