import assert from "assert";
import { FakeDatabaseDriver } from "./FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { Column } from "../../../lib/database/schema/Column";
import { TableID } from "../../../lib/database/schema/TableID";

describe("Migrator", () => {

    let databaseDriver!: FakeDatabaseDriver;
    let migration!: Migration;
    beforeEach(() => {
        databaseDriver = new FakeDatabaseDriver();
        migration = Migration.empty();
    });

    const testColumn = new Column(
        new TableID(
            "public",
            "some_table"
        ),
        "new_col",
        "text",
        "'nice'"
    );

    it("create columns", async() => {
        migration.create({
            columns: [
                testColumn
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);

        assert.deepStrictEqual(databaseDriver.columns, {
            "public.some_table.new_col": {
                name: "new_col",
                type: "text",
                "default": "'nice'"
            }
        });
    });

    it("drop columns", async() => {
        databaseDriver.createOrReplaceColumn(testColumn);

        migration.drop({
            columns: [
                testColumn
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);
        
        assert.deepStrictEqual(Object.values(databaseDriver.columns), []);
    });

});
