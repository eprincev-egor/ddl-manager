import assert from "assert";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { Column } from "../../../lib/database/schema/Column";
import { TableID } from "../../../lib/database/schema/TableID";
import { Select } from "../../../lib/ast";
import { TableReference } from "../../../lib/database/schema/TableReference";
import { Database } from "../../../lib/database/schema/Database";
import { UpdateMigrator } from "../../../lib/Migrator/UpdateMigrator";


describe("Migrator", () => {

    const timeoutOnDeadlock = UpdateMigrator.timeoutOnDeadlock;

    let databaseDriver!: FakeDatabaseDriver;
    let migration!: Migration;
    let database!: Database;
    beforeEach(() => {
        databaseDriver = new FakeDatabaseDriver();
        migration = Migration.empty();
        database = new Database();
    });

    afterEach(() => {
        UpdateMigrator.timeoutOnDeadlock = timeoutOnDeadlock;
    });

    const testColumn = new Column(
        new TableID(
            "public",
            "some_table"
        ),
        "new_col",
        "text",
        true,
        "'nice'"
    );

    it("create columns", async() => {
        migration.create({
            columns: [
                testColumn
            ]
        });

        await MainMigrator.migrate(databaseDriver, database, migration);

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

        await MainMigrator.migrate(databaseDriver, database, migration);
        
        assert.deepStrictEqual(Object.values(databaseDriver.columns), []);
    });

});
