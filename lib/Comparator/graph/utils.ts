import { CacheColumn } from "./CacheColumn";
import { flatMap } from "lodash";

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
    let matrix: CacheColumn[][] = [];

    const firstLevel = addCircularDependencies(rootColumns);
    matrix.push(firstLevel);

    addNextLevels(matrix, firstLevel);

    matrix = removeDuplicates(matrix);
    return matrix;
}

function addNextLevels(
    matrix: CacheColumn[][],
    prevLevel: CacheColumn[]
) {
    let nextLevel = flatMap(prevLevel, column => 
        column.findNotCircularUses()
    );
    nextLevel = addCircularDependencies(nextLevel);

    if ( nextLevel.length === 0 ) {
        return;
    }

    matrix.push(nextLevel);
    addNextLevels(
        matrix, nextLevel
    );
}

function addCircularDependencies(level: CacheColumn[]): CacheColumn[] {
    const output: CacheColumn[] = [];
    for (const column of level) {
        output.push(column);
        output.push(...column.findCircularUses());
    }
    return output;
}

function removeDuplicates(matrix: CacheColumn[][]) {
    const known: Record<string, boolean> = {};

    for (let n = matrix.length, i = n - 1; i >= 0; i--) {
        const line = matrix[i];
        const newLine: CacheColumn[] = [];

        for (let m = line.length, j = m - 1; j >= 0; j--) {
            const column = line[j];

            const isDuplicate = column.getId() in known;
            if ( isDuplicate ) {
                continue;
            }

            newLine.push(column);
            known[ column.getId() ] = true;
        }

        matrix[i] = newLine;
    }

    const newMatrix = matrix.filter(line => line.length > 0);
    return newMatrix;
}