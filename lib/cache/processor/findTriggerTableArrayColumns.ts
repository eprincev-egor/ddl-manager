import { Cache, Expression } from "../../ast";
import { ArrayElement } from "../../ast/expression/ArrayElement";
import { TableID } from "../../database/schema/TableID";
import { buildReferenceMeta } from "./buildReferenceMeta";

export function findTriggerTableArrayColumns(
    cache: Cache,
    inputTriggerTable?: TableID,
    expressions?: Expression[],
    arrayColumns: string[] = []
) {
    if ( cache.select.from.length === 0 ) {
        return [];
    }

    const triggerTable = inputTriggerTable || cache.getFromTable();
    expressions = expressions || buildReferenceMeta(cache, triggerTable).expressions;

    for (const expression of expressions) {
        if ( isArrayBinary(expression) ) {
            if ( expression.isEqualAny() ) {
                const [left] = expression.splitBy("=");
                const isTriggerTableColumnEqualAny = (
                    left.getColumnReferences().every(columnRef =>
                        columnRef.isRefTo(cache, triggerTable)
                    )
                );
                if ( isTriggerTableColumnEqualAny ) {
                    continue;
                }
            }

            if ( expression.isBinary("&&") ) {
                const notArrExpressions = expression.elements.filter(item =>
                    !(item instanceof ArrayElement)
                );

                for (const notArrayExpression of notArrExpressions) {
                    const mutableColumns = notArrayExpression.getColumnReferences()
                    .filter(columnRef =>
                        columnRef.name !== "id" &&
                        columnRef.isRefTo(cache, triggerTable)
                    )
                    .map(columnRef =>
                        columnRef.name
                    );

                    arrayColumns.push( ...mutableColumns );
                }

                continue;
            }

            const mutableColumns = expression.getColumnReferences()
                .filter(columnRef =>
                    columnRef.name !== "id" &&
                    columnRef.isRefTo(cache, triggerTable)
                )
                .map(columnRef =>
                    columnRef.name
                );

            arrayColumns.push( ...mutableColumns );
        }

        if ( expression.isBinary("or") ) {
            findTriggerTableArrayColumns(
                cache, triggerTable,
                expression.splitBy("or"),
                arrayColumns
            );
        }
    }

    return arrayColumns;
}

function isArrayBinary(expression: Expression): boolean {
    return (
        expression.isBinary("&&") ||
        expression.isBinary("@>") ||
        expression.isBinary("<@") ||
        expression.isEqualAny()
    );
}
