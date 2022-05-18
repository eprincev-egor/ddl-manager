import { ColumnReference, Expression, FuncCall } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayUnionAllAgg } from "./ArrayUnionAllAgg";

// TODO: this is should be plugin
export class ArrayUnionAgg extends AbstractAgg {

    readonly helpersAgg: ArrayUnionAllAgg[];
    private helperColumnName: string;

    constructor(params: IAggParams) {
        super(params);
        this.helperColumnName = "__" + params.columnName + "_arr";
        this.helpersAgg = [new ArrayUnionAllAgg({
            call: new FuncCall(
                "array_union_all_agg", params.call.args,
                params.call.where
            ),
            columnName: this.helperColumnName
        })];
    }

    minus() {
        return this.printTotal();
    }

    plus() {
        return this.printTotal();
    }

    private printTotal() {
        return Expression.funcCall(
            "cm_distinct_array", [
                Expression.unknown(this.helperColumnName, {
                    [this.helperColumnName]: new ColumnReference(
                        new TableReference(new TableID(
                            "",
                            ""
                        )),
                        this.helperColumnName
                    )
                })
            ]
        );
    }
}