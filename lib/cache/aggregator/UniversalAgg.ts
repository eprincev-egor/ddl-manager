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

    minus(value: Expression) {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("minus", value, null) }
    ) as ${ this.printAlias() }
)`);
    }

    plus(value: Expression) {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("plus", null, value) }
    ) as ${ this.printAlias() }
)`);
    }

    delta(prevValue: Expression, nextValue: Expression) {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations("delta", prevValue, nextValue) }
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

    private printChildrenAggregations(
        aggType: AggType,
        prevValue: Expression | null,
        nextValue: Expression | null
    ) {
        let sql: string = "";

        for (const arrayAgg of this.childAggregations) {
            const expression = this.callChildAgg(
                arrayAgg,
                aggType,
                prevValue,
                nextValue
            );

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

    private callChildAgg(
        arrayAgg: ArrayAgg,
        aggType: AggType,
        minusValue: Expression | null,
        plusValue: Expression | null
    ) {
        const columnRef = arrayAgg.call.getColumnReferences()[0] as ColumnReference;
        
        if ( aggType === "delta" && columnRef.name === "id" ) {
            return arrayAgg.total;
        }

        // const minusValue = Expression.unknown(`old.${ columnRef.name }`);
        // const plusValue = Expression.unknown(`new.${ columnRef.name }`);

        if ( aggType === "minus" ) {
            const sql = arrayAgg.minus( minusValue as Expression );
            return sql;
        }
        if ( aggType === "plus" ) {
            const sql = arrayAgg.plus( plusValue as Expression );
            return sql;
        }

        // delta
        const sql = arrayAgg.delta(
            minusValue as Expression,
            plusValue as Expression
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