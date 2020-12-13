import assert from "assert";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { DatabaseFunction } from "../../../lib/database/schema/DatabaseFunction";

describe("Migrator", () => {

    let databaseDriver!: FakeDatabaseDriver;
    let migration!: Migration;
    beforeEach(() => {
        databaseDriver = new FakeDatabaseDriver();
        migration = Migration.empty();
    });

    const testFunc = new DatabaseFunction({
        schema: "public",
        name: "some_simple_func",
        args: [],
        returns: {type: "void"},
        body: ""
    });

    it("create function", async() => {
        migration.create({
            functions: [
                testFunc
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);

        assert.strictEqual(
            databaseDriver.state.functions.length,
            1
        );
        assert.strictEqual(
            databaseDriver.state.functions[0].getSignature(),
            "public.some_simple_func()"
        );
    });

    it("drop function", async() => {
        
        databaseDriver.createOrReplaceFunction(testFunc);

        migration.drop({
            functions: [
                testFunc
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);

        assert.strictEqual(
            databaseDriver.state.functions.length,
            0
        );
    });

});
