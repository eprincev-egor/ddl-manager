import { uniq } from "lodash";
import { Expression } from "../../ast";
import { CacheContext } from "../trigger-builder/CacheContext";

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export function buildArrVars(
    context: CacheContext,
    prefix: string = "__"
): IArrVar[] {

    const arrayColumns = findTriggerTableArrayColumns(context);

    const dbTable = context.database.getTable(context.triggerTable);
    context.referenceMeta.columns.forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        if ( dbColumn && dbColumn.type.isArray() ) {
            arrayColumns.push(columnName);
        }
    });

    const arrVars: IArrVar[] = [];
    uniq(arrayColumns).forEach(columnName => {
        const dbColumn = dbTable && dbTable.getColumn(columnName);

        arrVars.push({
            name: prefix + columnName,
            type: dbColumn ? dbColumn.type.toString() : "bigint[]",
            triggerColumn: columnName
        });
    });

    return arrVars;
}

function findTriggerTableArrayColumns(
    context: CacheContext,
    expressions = context.referenceMeta.expressions,
    arrayColumns: string[] = []
) {
    for (const expression of expressions) {
        if ( isArrayBinary(expression) ) {
            const mutableColumns = expression.getColumnReferences()
                .filter(columnRef =>
                    columnRef.name !== "id" &&
                    context.isColumnRefToTriggerTable(columnRef)
                )
                .map(columnRef =>
                    columnRef.name
                );

            arrayColumns.push( ...mutableColumns );
        }

        if ( expression.isBinary("or") ) {
            findTriggerTableArrayColumns(
                context,
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
