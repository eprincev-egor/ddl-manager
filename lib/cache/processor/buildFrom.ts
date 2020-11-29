import { TableReference } from "../../database/schema/TableReference";
import { CacheContext } from "../trigger-builder/CacheContext";
import { findNecessaryTableMatrix } from "./findNecessaryTableMatrix";

export function buildFrom(context: CacheContext) {
    const necessaryTableMatrix: string[][] = findNecessaryTableMatrix(context);

    const fromItems: string[] = [context.triggerTable.toStringWithoutPublic()];

    for (const necessaryTableAliases of necessaryTableMatrix) {
        for (const identifier of necessaryTableAliases) {

            const refFilter = TableReference.identifier2filter(identifier);

            const tableReference = context.cache.select.findTableReference(refFilter);
            if ( !tableReference ) {
                continue;
            }
            
            if ( fromItems.includes(tableReference.toString()) ) {
                continue;
            }
            fromItems.push(tableReference.toString());
        }
    }

    return fromItems;
}
