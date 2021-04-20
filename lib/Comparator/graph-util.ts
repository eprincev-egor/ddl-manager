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
    const sortedSelectsForEveryColumn = allSelectsForEveryColumn
        .filter(item =>
            isRoot(allSelectsForEveryColumn, item)
        );

    for (const prevItem of sortedSelectsForEveryColumn) {

        // ищем те, которые явно указали, что они будут после prevItem
        const nextItems = allSelectsForEveryColumn.filter((nextItem) =>
            dependentOn(nextItem.select, {
                for: prevItem.for,
                column: prevItem.select.columns[0]
            })
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
    if ( item.select.columns[0]!.expression.getExplicitCastType() ) {
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
