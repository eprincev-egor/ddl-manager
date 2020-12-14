import assert from "assert";
import fs from "fs";
import { sleep } from "../sleep";
import { Migration } from "../../../lib/Migrator/Migration";
import { watcher } from "./utils/watcher";
import { Database } from "../../../lib/database/schema/Database";
import { Table } from "../../../lib/database/schema/Table";

describe("integration/FileWatcher watch change cache", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);

    const testCache = `
        cache totals for companies (
            select
                sum(orders.profit) as orders_profit
            from orders
            where
                orders.id_client = companies.id
        )
    `;
    const ordersTable = new Table(
        "public",
        "orders",
        []
    );
    const companiesTable = new Table(
        "public",
        "companies",
        []
    );

    it("create cache", async() => {
        
        const filePath = ROOT_TMP_PATH + "/some-cache.sql";
        
        const testDatabase = new Database([
            ordersTable
        ]);

        let migration!: Migration;
        let counter = 0;

        await watch((_migration) => {
            migration = _migration;
            counter++;
        }, testDatabase);
        
        fs.writeFileSync(filePath, testCache);
        await sleep(50);
        
        assert.equal(counter, 1);
        assert.strictEqual(migration.toCreate.functions.length, 1, "one func to create");
        assert.strictEqual(migration.toCreate.triggers.length, 1, "one trigger to create");
        assert.strictEqual(migration.toCreate.columns.length, 1, "one column to create");
        assert.strictEqual(migration.toCreate.updates.length, 0, "no updates to create");
    });

    it("drop cache", async() => {
        
        const filePath = ROOT_TMP_PATH + "/some-cache.sql";
        fs.writeFileSync(filePath, testCache);
   
        const testDatabase = new Database([
            ordersTable,
            companiesTable
        ]);

        let migration!: Migration;
        let counter = 0;
        await watch((_migration) => {
            migration = _migration;
            counter++;
        }, testDatabase);
        

        fs.unlinkSync(filePath);
        await sleep(50);

        assert.strictEqual(counter, 1);
        assert.strictEqual(migration.toDrop.functions.length, 1, "one func to drop");
        assert.strictEqual(migration.toDrop.triggers.length, 1, "one trigger to drop");
        assert.strictEqual(migration.toDrop.columns.length, 1, "one column to drop");
    });

});