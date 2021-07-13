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

    it("update columns", async() => {
        databaseDriver.setRowsCount("public.some_table", 1499);

        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(new TableID("public", "some_table"))
            }]
        });

        await MainMigrator.migrate(databaseDriver, database, migration);

        assert.deepStrictEqual(
            databaseDriver.getUpdatedPackages("public.some_table"),
            [{limit: 500}, {limit: 500}, {limit: 500}]
        );
    });

    it("update error on invalid select", async() => {
        databaseDriver.setRowsCount("public.some_table", 1499);
        databaseDriver.updateCachePackage = () => {
            throw new Error("operator does not exist: bigint[] && integer[]");
        };

        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(new TableID("public", "some_table"))
            }]
        });

        let actualError = new Error("expected error");
        try {
            await MainMigrator.migrate(databaseDriver, database, migration);
        } catch(err) {
            actualError = err;
        }

        assert.strictEqual(
            actualError.message,
            "operator does not exist: bigint[] && integer[]"
        );
    });

    it("retry update on deadlock", async() => {
        UpdateMigrator.timeoutOnDeadlock = 1;
        databaseDriver.setRowsCount("public.some_table", 1499);

        const originalUpdate = databaseDriver.updateCachePackage;
        databaseDriver.updateCachePackage = () => {
            databaseDriver.updateCachePackage = originalUpdate;
            throw new Error("deadlock");
        };

        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(new TableID("public", "some_table"))
            }]
        });

        await MainMigrator.migrate(databaseDriver, database, migration);

        assert.deepStrictEqual(
            databaseDriver.getUpdatedPackages("public.some_table"),
            [{limit: 500}, {limit: 500}, {limit: 500}]
        );
    });

    it("throw deadlock error, try update many times", async() => {
        UpdateMigrator.timeoutOnDeadlock = 1;
        
        databaseDriver.setRowsCount("public.some_table", 499);

        let updatesCount = 0;
        databaseDriver.updateCachePackage = () => {
            updatesCount++;

            if ( updatesCount < 10 ) {
                throw new Error("deadlock");
            }

            return Promise.resolve(499);
        };

        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(new TableID("public", "some_table"))
            }]
        });

        
        await MainMigrator.migrate(databaseDriver, database, migration);

        assert.strictEqual(
            updatesCount,
            10
        );
    });
});
