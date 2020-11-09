import { findMinimalRoute } from "./findMinimalRoute";
import { Cache, Expression, Table } from "../../ast";

export function findNecessaryTableMatrix(
    cache: Cache,
    triggerTable: Table,
    conditionsMatrix: Expression[][]
) {
    const linksToChangedTable = cache.select.findTableReferences(triggerTable);
    const necessaryTableMatrix: string[][] = [];

    for (const andConditions of conditionsMatrix) {
        const necessaryTableAliases: string[] = [];

        const graph = andConditions.map(condition => {
            const points: string[] = condition.getColumnReferences()
                .map(columnRef => columnRef.tableReference.getIdentifier());
            return points;
        });

        for (const changedTableInSelect of linksToChangedTable) {
            const minimalRoute = findMinimalRoute({
                graph,
                start: changedTableInSelect.getIdentifier(),
                end: cache.for.getIdentifier()
            });
    
            if ( !minimalRoute ) {
                continue;
            }

            const middlePoints = minimalRoute.slice(1, -1);
            for (const middlePoint of middlePoints) {

                if ( necessaryTableAliases.includes(middlePoint) ) {
                    continue;
                }

                necessaryTableAliases.push(middlePoint);
            }
        }

        necessaryTableMatrix.push(necessaryTableAliases);
    }
    
    return necessaryTableMatrix;
}
