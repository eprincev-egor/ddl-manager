import assert from "assert";
import { Cache, Select } from "../../ast";

export interface ISortSelectItem {
    select: Select;
    cache: Cache;
}


export function sortSelectsByDependencies(allSelectsForEveryColumn: ISortSelectItem[]) {

    // sort selects be dependencies
    const sortedSelectsForEveryColumn = allSelectsForEveryColumn
        .filter(item =>
            isRoot(allSelectsForEveryColumn, item)
        );

    for (const prevItem of sortedSelectsForEveryColumn) {

        // ищем те, которые явно указали, что они будут после prevItem
        const nextItems = allSelectsForEveryColumn.filter((nextItem) =>
            dependentOn(nextItem, prevItem)
        );

        for (let j = 0, m = nextItems.length; j < m; j++) {
            const nextItem = nextItems[ j ];

            // если в очереди уже есть этот элемент
            const index = sortedSelectsForEveryColumn.indexOf(nextItem);
            //  удалим дубликат
            if ( index !== -1 ) {
                sortedSelectsForEveryColumn.splice(index, 1);
            }

            //  и перенесем в конец очереди,
            //  таким образом, если у элемента есть несколько "after"
            //  то он будет постоянно уходить в конец после всех своих "after"
            sortedSelectsForEveryColumn.push(nextItem);
        }
    }

    return sortedSelectsForEveryColumn;
}

function isRoot(allItems: ISortSelectItem[], item: ISortSelectItem) {
    const hasDependencies = allItems.some(prevItem =>
        prevItem !== item &&
        dependentOn(item, prevItem)
    );
    return !hasDependencies;
}

// x dependent on y ?
function dependentOn(
    xItem: ISortSelectItem,
    yItem: ISortSelectItem
): boolean {
    
    const xColumn = xItem.select.columns[0];
    const yColumn = yItem.select.columns[0];

    assert.ok(xColumn);
    assert.ok(yColumn);

    const xRefs = xColumn.expression.getColumnReferences();
    const xDependentOnY = xRefs.some(xColumnRef =>
        xColumnRef.tableReference.table.equal( yItem.cache.for.table ) &&
        xColumnRef.name === yColumn.name
    );

    return xDependentOnY;
}