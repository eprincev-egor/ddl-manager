import {
    Expression,
    Table,
    Cache,
    TableReference
} from "../../ast";
import { IReferenceMeta } from "./buildReferenceMeta";

export function buildSimpleWhere(
    cache: Cache,
    triggerTable: Table,
    row: "new" | "old",
    referenceMeta: IReferenceMeta
) {
    const linksToTriggerTable = cache.select.findTableReferences(triggerTable);

    const conditions = referenceMeta.expressions.map(expression => {

        linksToTriggerTable.forEach((linkToTriggerTable) => {

            expression = expression.replaceTable(
                linkToTriggerTable,
                new TableReference(
                    triggerTable,
                    row
                )
            );
        });

        return expression;
    });

    const where = Expression.and(conditions);
    if ( !where.isEmpty() ) {
        return where;
    }
}
