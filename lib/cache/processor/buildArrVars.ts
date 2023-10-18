import { uniq } from "lodash";
import { CacheContext } from "../trigger-builder/CacheContext";
import { findTriggerTableArrayColumns } from "./findTriggerTableArrayColumns";

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export function buildArrVars(
    context: CacheContext,
    prefix: string = "__"
): IArrVar[] {

    const arrayColumns = findTriggerTableArrayColumns(
        context.cache,
        context.triggerTable,
        context.referenceMeta.expressions
    );

    const arrVars: IArrVar[] = [];
    uniq(arrayColumns).forEach(columnName => {
        arrVars.push({
            name: prefix + columnName,
            type: "bigint[]",
            triggerColumn: columnName
        });
    });

    return arrVars;
}
