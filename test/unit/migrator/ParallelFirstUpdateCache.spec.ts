import { UpdateMigrator, parallelPackagesCount } from "../../../lib/Migrator/UpdateMigrator";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { IChanges, Migration } from "../../../lib/Migrator/Migration";
import { TableID } from "../../../lib/database/schema/TableID";
import { Select } from "../../../lib/ast";
import { TableReference } from "../../../lib/database/schema/TableReference";
import { Database } from "../../../lib/database/schema/Database";
import { CacheUpdate } from "../../../lib/Comparator/graph/CacheUpdate";
import { CacheColumn } from "../../../lib/Comparator/graph/CacheColumn";
import { sleep } from "../../integration/sleep";
import assert from "assert";


describe("ParallelFirstUpdateCache", () => {

    let fakePostgres!: FakeDatabaseDriver;
    let migration!: Migration;
    let database!: Database;

    const timeoutOnDeadlock = UpdateMigrator.timeoutOnDeadlock;
    const packageSize = 100;
    const maxId = 2 * packageSize * parallelPackagesCount;

    const someTable = new TableID("public", "some_table");
    const someUpdate = new CacheUpdate([
        new CacheColumn({
            for: new TableReference(someTable),
            name: "my_column",
            cache: {name: "my_cache", signature: "my_cache for some_table"},
            select: new Select()
        })
    ]);
    const someUpdateMigration: Partial<IChanges> = {
        updates: [someUpdate]
    };
    UpdateMigrator.timeoutOnDeadlock = 1;

    beforeEach(() => {
        fakePostgres = new FakeDatabaseDriver();
        database = new Database();
        
        fakePostgres.setTableMinMax(someTable, 1, maxId);

        migration = Migration.empty();
        migration.create(someUpdateMigration);
    });

    it("update all rows", async() => {
        await MainMigrator.migrate(fakePostgres, database, migration);

        const actualUpdates = fakePostgres.getUpdates(someTable);
        assert.strictEqual(
            actualUpdates.length,
            Math.ceil(maxId / packageSize)
        );
    });

    it("update all rows when recursionWith is empty array (parallel update from end to start)", async() => {
        migration = Migration.empty();
        migration.create({
            updates: [someUpdate]
        });

        await MainMigrator.migrate(fakePostgres, database, migration);

        const actualUpdatedIds = fakePostgres.getUpdates(someTable);

        assert.deepStrictEqual(actualUpdatedIds, [
            "101 - 200",
            "301 - 400",
            "501 - 600",
            "701 - 800",
            "901 - 1000",
            "1101 - 1200",
            "1301 - 1400",
            "1501 - 1600",
            "1 - 100",
            "201 - 300",
            "401 - 500",
            "601 - 700",
            "801 - 900",
            "1001 - 1100",
            "1201 - 1300",
            "1401 - 1500"
        ]);
    });

    it("re-try on deadlock", async() => {
        UpdateMigrator.timeoutOnDeadlock = 1;

        const originalUpdate = fakePostgres.updateCacheForRows;
        fakePostgres.updateCacheForRows = () => {
            fakePostgres.updateCacheForRows = originalUpdate;

            const deadlockError = new Error("dead lock") as any;
            deadlockError.code = "40P01";
            throw deadlockError;
        };

        await MainMigrator.migrate(fakePostgres, database, migration);

        const actualUpdates = fakePostgres.getUpdates(someTable);
        assert.strictEqual(
            actualUpdates.length,
            Math.ceil(maxId / packageSize)
        );

        UpdateMigrator.timeoutOnDeadlock = timeoutOnDeadlock;
    });

    it("need parallel update", async() => {
        const calls: string[] = [];

        const originalUpdate = fakePostgres.updateCacheForRows;
        fakePostgres.updateCacheForRows = async (...args) => {
            calls.push("start");
            
            await sleep(10);
            const result = await originalUpdate.apply(fakePostgres, args);
            
            calls.push("end");
            return result;
        };

        await MainMigrator.migrate(fakePostgres, database, migration);

        assert.deepStrictEqual(
            calls.slice(0, parallelPackagesCount),
            repeat("start", parallelPackagesCount),
            "start parallel"
        );
        assert.deepStrictEqual(
            calls.slice(-parallelPackagesCount),
            repeat("end", parallelPackagesCount),
            "end parallel"
        );
    });

    it("ignore update error on invalid select", async() => {
        fakePostgres.updateCacheForRows = () => {
            throw new Error("operator does not exist: bigint[] && integer[]");
        };

        migration.create({
            updates: [someUpdate]
        });

        await MainMigrator.migrate(fakePostgres, database, migration);

        assert.ok(true, "no errors");
    });

    function repeat(word: string, quantity: number): string[] {
        const words: string[] = [];
        for (let i = 0; i < quantity; i++) {
            words.push(word);
        }
        return words;
    }
});
