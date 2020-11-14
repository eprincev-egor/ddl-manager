import { FuncCall, Expression, IExpressionElement, Select, SelectColumn } from "../../ast";

export interface IAggParams {
    select: Select;
    updateColumn: SelectColumn;
    call: FuncCall;
    total: Expression;
}

export abstract class AbstractAgg {

    readonly call: FuncCall;
    protected readonly total: Expression;
    protected readonly recalculateSelect: string;

    constructor(params: IAggParams) {
        this.call = params.call;
        this.total = params.total;
        this.recalculateSelect = params.select.cloneWith({
            columns: [params.updateColumn]
        }).toString();
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