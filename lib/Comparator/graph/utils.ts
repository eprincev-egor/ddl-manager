import { CacheColumn } from "./CacheColumn";
import { flatMap, flatten } from "lodash";

export function groupByTables(columns: CacheColumn[]) {
    const byTables: Record<string, CacheColumn[]> = {};

    for (const column of columns) {
        const table = column.getTableId();

        const tableColumns = byTables[ table ] || [];
        byTables[ table ] = tableColumns;

        tableColumns.push(column);
    }

    return byTables;
}

export function buildDependencyMatrix(
    rootColumns: CacheColumn[]
): CacheColumn[][] {
    const matrix: CacheColumn[][] = [];

    addNextLevels(matrix, rootColumns);
    return removeDuplicates(matrix);
}

function addNextLevels(
    matrix: CacheColumn[][],
    prevLevel: CacheColumn[]
) {
    const circularLevels = splitByCircularDeps(prevLevel);
    matrix.push(...circularLevels);

    const nextLevel = flatMap(flatten(circularLevels), column => 
        column.findNotCircularUses()
    );
    if ( nextLevel.length > 0 ) {
        addNextLevels(
            matrix, nextLevel
        );
    }
}

function splitByCircularDeps(level: CacheColumn[]) {
    const prevLevel: CacheColumn[] = [];
    const nextLevel: CacheColumn[] = [];

    for (const column of level) {
        const circularDeps = column.findCircularUses();
        if ( circularDeps.length === 0 ) {
            prevLevel.push(column);
            continue;
        }

        if ( column.hasForeignTablesDeps() ) {
            prevLevel.push(column);
            nextLevel.push(...circularDeps);
        }
        else {
            prevLevel.push(...circularDeps);
            nextLevel.push(column);
        }
    }

    return [prevLevel, nextLevel].filter(level => 
        level.length > 0
    );
}

function removeDuplicates(matrix: CacheColumn[][]) {
    const known: Record<string, boolean> = {};
    let newMatrix: CacheColumn[][] = [];

    for (let n = matrix.length, i = n - 1; i >= 0; i--) {
        const line = matrix[i];
        const newLine: CacheColumn[] = [];

        for (let m = line.length, j = m - 1; j >= 0; j--) {
            const column = line[j];

            const isDuplicate = column.getId() in known;
            if ( isDuplicate ) {
                continue;
            }

            newLine.unshift(column);
            known[ column.getId() ] = true;
        }

        newMatrix[i] = newLine;
    }

    newMatrix = newMatrix.filter(line => line.length > 0);
    return newMatrix;
}