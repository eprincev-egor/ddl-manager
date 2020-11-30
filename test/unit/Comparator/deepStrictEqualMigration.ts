import _ from "lodash";
import assert from "assert";
import { Migration, IChanges } from "../../../lib/Migrator/Migration";

export function deepStrictEqualMigration(
    actualMigration: Migration,
    expectedMigration: Partial<{
        create: Partial<IChanges>;
        drop: Partial<IChanges>;
    }>
) {
    const expectedDiff = Migration.empty()
        .create({
            ...expectedMigration.create
        })
        .drop({
            ...expectedMigration.drop
        })
    ;
    
    assert.deepStrictEqual(actualMigration, expectedDiff);
}
