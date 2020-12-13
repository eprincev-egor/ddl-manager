import assert from "assert";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { Migration } from "../../../lib/Migrator/Migration";
import { DatabaseTrigger } from "../../../lib/database/schema/DatabaseTrigger";
import { TableID } from "../../../lib/database/schema/TableID";


describe("Migrator", () => {

    let databaseDriver!: FakeDatabaseDriver;
    let migration!: Migration;
    beforeEach(() => {
        databaseDriver = new FakeDatabaseDriver();
        migration = Migration.empty();
    });

    const testTrigger = new DatabaseTrigger({
        name: "some_simple_trigger",
        table: new TableID(
            "public",
            "some_table"
        ),
        procedure: {
            schema: "public",
            name: "some_trigger_func",
            args: []
        }
    })

    it("create trigger", async() => {

        migration.create({
            triggers: [
                testTrigger
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);

        assert.strictEqual(
            databaseDriver.state.triggers.length,
            1
        );
        assert.strictEqual(
            databaseDriver.state.triggers[0].getSignature(),
            "some_simple_trigger on public.some_table"
        );
    });

    it("drop trigger", async() => {

        databaseDriver.createOrReplaceTrigger(testTrigger);

        migration.drop({
            triggers: [
                testTrigger
            ]
        });

        await MainMigrator.migrate(databaseDriver, migration);

        assert.strictEqual(
            databaseDriver.state.triggers.length,
            0
        );
    });

});
