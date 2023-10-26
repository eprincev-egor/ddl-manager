import { strict } from "assert";

export function deepEqualRows(
    actualRows: Record<string, any>[] | undefined,
    expectedRows: Record<string, any>[]
) {
    strict.ok(actualRows, "required rows");
    strict.equal(
        actualRows.length, expectedRows.length,
        "expected rows count"
    );

    actualRows.sort((a, b) => a.id - b.id).forEach((actualRow, i) =>
        deepEqualRow(actualRow, expectedRows[i])
    );
}

export function deepEqualRow(
    actualRow: Record<string, any> | undefined,
    expectedRow: Record<string, any>
) {
    strict.ok(actualRow, "required row");

    const keys = Object.keys(expectedRow);
    strict.deepEqual(
        pick(actualRow, ...keys),
        expectedRow
    );
}

export function pick(row: Record<string, any>, ...keys: string[]) {
    const partial: Record<string, any> = {};
    for (const key of keys) {
        partial[ key ] = row[ key ];
    }
    return partial;
}

export function shouldBeBetween(
    dateIso: string | undefined,
    start: Date,
    end: Date
) {
    strict.ok(dateIso, "required date");

    const scanDate = new Date(dateIso);                
    strict.ok(
        +scanDate >= +start,
        `expected greater than ${start}`
    );
    strict.ok(
        +scanDate <= +end,
        `expected less than ${end}`
    );
}