import { Cache, Select, SelectColumn } from "../ast";
import { TableReference } from "../database/schema/TableReference";
import { IUpdate } from "../Migrator/Migration";

export interface ISortSelectItem {
    select: Select;
    for: TableReference;
    cache: Cache;
}


export function sortSelectsByDependencies(allSelectsForEveryColumn: ISortSelectItem[]) {

    // sort selects be dependencies
    let sortedSelectsForEveryColumn = allSelectsForEveryColumn
        .filter(item =>
            isRoot(allSelectsForEveryColumn, item)
        );

    for (let prevIndex = 0; prevIndex < sortedSelectsForEveryColumn.length; prevIndex++) {
        const prevItem = sortedSelectsForEveryColumn[prevIndex];

        // ищем те, которые явно указали, что они будут после prevItem
        const nextItems = allSelectsForEveryColumn.filter((nextItem) =>
            !isExplicitCastType(nextItem) &&
            nextItem !== prevItem &&
            dependentOn(nextItem.select, {
                for: prevItem.for,
                column: prevItem.select.columns[0]
            })
        );

        for (const nextItem of nextItems) {
            // если в очереди уже есть этот элемент
            const nextIndex = sortedSelectsForEveryColumn.indexOf(nextItem);
            //  удалим дубликат
            if ( nextIndex !== -1 ) {
                if ( nextIndex > prevIndex ) {
                    sortedSelectsForEveryColumn.splice(nextIndex, 1);
                }
                else {
                    delete sortedSelectsForEveryColumn[ nextIndex ];
                }
            }

            //  и перенесем в конец очереди,
            //  таким образом, если у элемента есть несколько "after"
            //  то он будет постоянно уходить в конец после всех своих "after"
            sortedSelectsForEveryColumn.push(nextItem);
        }
    }

    sortedSelectsForEveryColumn = sortedSelectsForEveryColumn.filter(item => !!item);
    return sortedSelectsForEveryColumn;
}

function isRoot(allItems: ISortSelectItem[], item: ISortSelectItem) {
    if ( isExplicitCastType(item) ) {
        return true;
    }

    const hasDependencies = allItems.some(prevItem =>
        prevItem !== item &&
        dependentOn(item.select, {
            for: prevItem.for,
            column: prevItem.select.columns[0]
        })
    );
    return !hasDependencies;
}

function isExplicitCastType(item: ISortSelectItem) {
    const explicitCastType = item.select.columns[0]!
        .expression.getExplicitCastType();
    return !!explicitCastType;
}

export function findRecursionUpdates(
    someUpdate: IUpdate,
    allUpdates: IUpdate[]
): IUpdate[] {
    for (const otherUpdate of allUpdates) {
        if ( otherUpdate == someUpdate ) {
            continue;
        }

        if ( hasCircularDependency(someUpdate, otherUpdate) ) {
            return [otherUpdate];
        }
    }

    return [];
}

function hasCircularDependency(
    xUpdate: IUpdate,
    yUpdate: IUpdate
) {
    for (const xColumn of xUpdate.select.columns) {
        for (const yColumn of yUpdate.select.columns) {
            const isCircularDependency = (
                dependentOn(
                    xUpdate.select, {
                        for: yUpdate.forTable,
                        column: yColumn
                    }
                )
                &&
                dependentOn(
                    yUpdate.select, {
                        for: xUpdate.forTable,
                        column: xColumn
                    }
                )
            );
            if ( isCircularDependency ) {
                return true;
            }
        }
    }

    return false;
}

// x dependent on y ?
function dependentOn(
    xSelect: Select,
    yItem: {for: TableReference, column: SelectColumn}
): boolean {
    const xRefs = xSelect.getAllColumnReferences();
    const xDependentOnY = xRefs.some(xColumnRef =>
        xColumnRef.tableReference.table.equal( yItem.for.table ) &&
        xColumnRef.name === yItem.column.name
    );

    return xDependentOnY;
}
