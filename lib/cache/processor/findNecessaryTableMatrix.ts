import { findMinimalRoute } from "./findMinimalRoute";
import { Expression } from "../../ast";
import { CacheContext } from "../trigger-builder/CacheContext";
import { buildConditionsMatrix } from "./buildConditionsMatrix";

export function findNecessaryTableMatrix(context: CacheContext) {
    const conditionsMatrix: Expression[][] = buildConditionsMatrix(context.cache);

    const linksToChangedTable = context.getTableReferencesToTriggerTable();
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
                end: context.cache.for.getIdentifier()
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
