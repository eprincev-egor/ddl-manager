import { noChanges } from "./noChanges";
import { CacheContext } from "../CacheContext";

export function noReferenceChanges(
    context: CacheContext
) {
    const importantColumns = context.referenceMeta.columns.slice();

    for (const filter of context.referenceMeta.filters) {
        const filterColumns = filter.getColumnReferences().map(columnRef =>
            columnRef.name
        );
        importantColumns.push( ...filterColumns );
    }

    return noChanges(
        context,
        importantColumns
    );
}
