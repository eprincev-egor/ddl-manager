import assert from "assert";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { TableID } from "../../../lib/database/schema/TableID";
import { Database } from "../../../lib/database/schema/Database";
import { Index } from "../../../lib/database/schema/Index";

describe("Migrator", () => {

    let databaseDriver!: FakeDatabaseDriver;
    let migration!: Migration;
    let database!: Database;
    beforeEach(() => {
        databaseDriver = new FakeDatabaseDriver();
        migration = Migration.empty();
        database = new Database();
    });

    const testIndex = new Index({
        name: "my_index",
        table: new TableID("public", "orders"),
        index: "btree",
        columns: ["id_client"]
    });

    it("create index", async() => {
        migration.create({
            indexes: [
                testIndex
            ]
        });

        await MainMigrator.migrate(databaseDriver, database, migration);

        assert.deepStrictEqual(databaseDriver.indexes, {
            "public.orders": [testIndex]
        });
    });

    it("drop index", async() => {
        await databaseDriver.createOrReplaceIndex(testIndex);

        migration.drop({
            indexes: [
                testIndex
            ]
        });

        await MainMigrator.migrate(databaseDriver, database, migration);

        assert.deepStrictEqual(databaseDriver.indexes, {
            "public.orders": []
        });
    });

});
