import assert from "assert";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { MainMigrator } from "../../../lib/Migrator/MainMigrator";
import { IChanges, IUpdate, Migration } from "../../../lib/Migrator/Migration";
import { TableID } from "../../../lib/database/schema/TableID";
import { Select } from "../../../lib/ast";
import { TableReference } from "../../../lib/database/schema/TableReference";
import { Database } from "../../../lib/database/schema/Database";
import { UpdateMigrator, packageSize, parallelPackagesCount } from "../../../lib/Migrator/UpdateMigrator";
import { sleep } from "../../integration/sleep";


describe("ParallelFirstUpdateCache", () => {

    let fakePostgres!: FakeDatabaseDriver;
    let migration!: Migration;
    let database!: Database;

    const timeoutOnDeadlock = UpdateMigrator.timeoutOnDeadlock;

    const maxId = 79999;

    const someTable = new TableID("public", "some_table");
    const someUpdate: IUpdate = {
        cacheName: "my_cache",
        select: new Select(),
        forTable: new TableReference(someTable),
        recursionWith: []
    };
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

    it("update all rows when recursionWith is empty array", async() => {
        migration = Migration.empty();
        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(someTable),
                recursionWith: []
            }]
        });

        await MainMigrator.migrate(fakePostgres, database, migration);

        const actualUpdatedIds = fakePostgres.getUpdates(someTable);
        assert.deepStrictEqual(actualUpdatedIds, [
            "1 - 10001",
            "5001 - 10001",
            "10001 - 20001",
            "15001 - 20001",
            "20001 - 30001",
            "25001 - 30001",
            "30001 - 40001",
            "35001 - 40001",
            "40001 - 50001",
            "45001 - 50001",
            "50001 - 60001",
            "55001 - 60001",
            "60001 - 70001",
            "65001 - 70001",
            "70001 - 80000",
            "75001 - 80000"
        ]);
    });

    it("re-try on deadlock", async() => {
        UpdateMigrator.timeoutOnDeadlock = 1;

        const originalUpdate = fakePostgres.updateCacheForRows;
        fakePostgres.updateCacheForRows = () => {
            fakePostgres.updateCacheForRows = originalUpdate;
            throw new Error("Deadlock");
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

    it("update error on invalid select", async() => {
        fakePostgres.updateCacheForRows = () => {
            throw new Error("operator does not exist: bigint[] && integer[]");
        };

        migration.create({
            updates: [{
                cacheName: "my_cache",
                select: new Select(),
                forTable: new TableReference(new TableID("public", "some_table")),
                recursionWith: []
            }]
        });

        let actualError = new Error("expected error");
        try {
            await MainMigrator.migrate(fakePostgres, database, migration);
        } catch(err) {
            actualError = err;
        }

        assert.strictEqual(
            actualError.message,
            "operator does not exist: bigint[] && integer[]"
        );
    });

    function generateIds(quantity: number): number[] {
        const ids: number[] = [];
        for (let id = 1; id <= quantity; id++) {
            ids.push(id);
        }
        return ids;
    }

    function repeat(word: string, quantity: number): string[] {
        const words: string[] = [];
        for (let i = 0; i < quantity; i++) {
            words.push(word);
        }
        return words;
    }
});
