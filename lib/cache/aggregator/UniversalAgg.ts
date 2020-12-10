import { Expression, ColumnReference, Spaces } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";

type AggType = "minus" | "plus" | "delta";

export class UniversalAgg extends AbstractAgg {

    private childAggregations: ArrayAgg[];

    constructor(params: IAggParams, childAggregations: ArrayAgg[]) {
        super(params);
        this.childAggregations = childAggregations;
    }

    minus() {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("minus") }
    ) as ${ this.printAlias() }
)`);
    }

    plus() {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("plus") }
    ) as ${ this.printAlias() }
)`);
    }

    delta() {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("delta") }
    ) as ${ this.printAlias() }
)`);
    }

    private printMainAgg() {
        const columnRef = this.call.getColumnReferences()[0] as ColumnReference;
        const tableRef = columnRef.tableReference;

        const preparedCall = this.call.replaceTable(tableRef, new TableReference(
            tableRef.table,
            "item"
        ));
        const sql = preparedCall.toSQL(
            Spaces.empty()
        );

        const spaces = Spaces.empty()
            .plusOneLevel()
            .plusOneLevel();
        const lines = sql.split("\n").map(line =>
            spaces + line
        );
        return lines.join("\n");
    }

    private printChildrenAggregations(aggType: AggType) {
        let sql: string = "";

        for (const arrayAgg of this.childAggregations) {
            const expression = this.callChildAgg(arrayAgg, aggType);

            if ( sql ) {
                sql += ",\n";
            }
            sql += expression.toString();
        }

        const spaces = Spaces.empty()
            .plusOneLevel()
            .plusOneLevel();
        const lines = sql.split("\n").map(line =>
            spaces + line
        );
        return lines.join("\n");
    }

    private callChildAgg(arrayAgg: ArrayAgg, aggType: AggType) {
        const columnRef = arrayAgg.call.getColumnReferences()[0] as ColumnReference;
        
        if ( aggType === "delta" && columnRef.name === "id" ) {
            return arrayAgg.total;
        }

        const minusValue = Expression.unknown(`old.${ columnRef.name }`);
        const plusValue = Expression.unknown(`new.${ columnRef.name }`);

        if ( aggType === "minus" ) {
            const sql = arrayAgg.minus( minusValue );
            return sql;
        }
        if ( aggType === "plus" ) {
            const sql = arrayAgg.plus( plusValue );
            return sql;
        }

        // delta
        const sql = arrayAgg.delta(
            minusValue,
            plusValue
        );
        return sql;
    }

    private printAlias() {
        const columns = this.childAggregations.map(arrayAgg => {
            const columnRef = arrayAgg.call.getColumnReferences()[0] as ColumnReference;
            return columnRef.name;
        });
        return `item(${ columns.join(", ") })`
    }
}