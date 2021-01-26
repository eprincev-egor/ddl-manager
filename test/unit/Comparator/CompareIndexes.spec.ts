import assert from "assert";
import _ from "lodash";
import { MainComparator } from "../../../lib/Comparator/MainComparator";
import { Comment } from "../../../lib/database/schema/Comment";
import { Database } from "../../../lib/database/schema/Database";
import { Index } from "../../../lib/database/schema/Index";
import { Table } from "../../../lib/database/schema/Table";
import { TableID } from "../../../lib/database/schema/TableID";
import { FilesState } from "../../../lib/fs/FilesState";
import { FileParser } from "../../../lib/parser";
import { FakeDatabaseDriver } from "../FakeDatabaseDriver";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import { someFileParams, testTableWithCache } from "./fixture/cache-fixture";

describe("Comparator: compare indexes", async() => {
    
    let postgres!: FakeDatabaseDriver;
    let database!: Database;
    let fs!: FilesState;
    beforeEach(() => {
        database = new Database();
        fs = new FilesState();
        postgres = new FakeDatabaseDriver();
    });

    it("sync empty state", async() => {
        const migration = await MainComparator.compare(postgres, database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                indexes: []
            },
            create: {
                indexes: []
            }
        });
    });

    const testIndex = new Index({
        name: "companies_max_order_id_cidx",
        index: "btree",
        table: new TableID("public", "companies"),
        columns: ["max_order_id"],
        comment: Comment.fromFs({
            objectType: "index",
            cacheSignature: "cache totals for companies"
        })
    });
    const testCache = FileParser.parseCache(`
        cache totals for companies (
            select
                max(orders.id) as max_order_id
            from orders
            where
                orders.id_client = companies.id
        )
        index btree on (max_order_id)
    `);

    it("create simple index", async() => {

        fs.addFile({
            ...someFileParams,
            content: {
                cache: [testCache]
            }
        });

        const migration = await MainComparator.compare(postgres, database, fs);

        assert.deepStrictEqual(migration.toDrop.indexes, []);
        assert.deepStrictEqual(migration.toCreate.indexes, [
            testIndex
        ]);
    });

    it("drop index", async() => {
        const testTableCompanies = new Table(
            "public", "companies"
        );
        database.setTable(testTableCompanies);
        database.addIndex(testIndex);


        const migration = await MainComparator.compare(postgres, database, fs);

        assert.deepStrictEqual(migration.toCreate.indexes, []);
        assert.deepStrictEqual(migration.toDrop.indexes, [
            testIndex
        ]);
    });

    it("no changes => empty migration", async() => {

        database.setTable(testTableWithCache);
        database.addIndex(testIndex);

        fs.addFile({
            ...someFileParams,
            content: {
                cache: [testCache]
            }
        });


        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

        assert.strictEqual(toDrop.indexes.length, 0, "no indexes to drop");
        assert.strictEqual(toCreate.indexes.length, 0, "no indexes to create");
    });

    it("don't drop frozen index", async() => {

        const frozenIndex = new Index({
            name: "companies_max_order_id_cidx",
            index: "btree",
            table: new TableID("public", "companies"),
            columns: ["max_order_id"],
            comment: Comment.frozen("index")
        });
        const testTableCompanies = new Table(
            "public", "companies"
        );
        database.setTable(testTableCompanies);
        database.addIndex(frozenIndex);

        const {toDrop, toCreate} = await MainComparator.compare(postgres, database, fs);

        assert.strictEqual(toDrop.indexes.length, 0, "no indexes to drop");
        assert.strictEqual(toCreate.indexes.length, 0, "no indexes to create");
    });

});