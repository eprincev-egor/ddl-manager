import { Expression, ColumnReference, Spaces, IColumnsMap, IExpressionElement } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";

export class UniversalAgg extends AbstractAgg {

    readonly helpersAgg: ArrayAgg[];

    constructor(params: IAggParams, childAggregations: ArrayAgg[]) {
        super(params);
        this.helpersAgg = childAggregations;
    }

    minus() {
        return this.printTotal();
    }

    plus(total: IExpressionElement, value: Expression) {
        return this.printTotal();
    }

    private printTotal() {
        return Expression.unknown(`(
    select
${ this.printMainAgg() }

    from unnest(
${ this.printChildrenAggregations() }
    ) as ${ this.printAlias() }
)`, this.buildColumnsMap());
    }

    private printMainAgg() {
        const mainAggCall = this.call.withoutWhere();
        let preparedCall = mainAggCall;

        for (const {tableReference} of mainAggCall.getColumnReferences()) {
            const itemRef = new TableReference(
                tableReference.table,
                "item"
            );

            preparedCall = preparedCall.replaceTable(
                tableReference, itemRef
            );
        }
        
        const sql = preparedCall.toSQL(
            Spaces.empty()
        );

        const spaces = Spaces.level(2);
        const lines = sql.split("\n").map(line =>
            spaces + line
        );
        return lines.join("\n");
    }

    private printChildrenAggregations() {
        let sql: string = "";

        for (const arrayAgg of this.helpersAgg) {
            if ( sql ) {
                sql += ",\n";
            }
            sql += arrayAgg.columnName.toString();
        }

        const spaces = Spaces.level(2);
        const lines = sql.split("\n").map(line =>
            spaces + line
        );
        return lines.join("\n");
    }

    private printAlias() {
        const columns = this.helpersAgg.map(arrayAgg => {
            const columnRef = arrayAgg.call.getColumnReferences()[0] as ColumnReference;
            return columnRef.name;
        });
        return `item(${ columns.join(", ") })`
    }

    private buildColumnsMap() {
        const columnsMap: IColumnsMap = {};

        for (const helperAgg of this.helpersAgg) {
            const aggColumnName = helperAgg.columnName.toString();

            columnsMap[ aggColumnName ] = new ColumnReference(
                new TableReference(new TableID(
                    "",
                    ""
                )),
                aggColumnName
            );
        }

        return columnsMap;
    }
}