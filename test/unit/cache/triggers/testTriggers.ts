import fs from "fs";
import path from "path";
import assert from "assert";
import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { Database } from "../../../../lib/database/schema/Database";
import { Table } from "../../../../lib/database/schema/Table";
import { Column } from "../../../../lib/database/schema/Column";
import { TableID } from "../../../../lib/database/schema/TableID";

export interface ITest {
    testDir: string;
    tables: string[];
}

export function testTriggers(test: ITest) {
    
    const cacheFilePath = path.join(test.testDir, "cache.sql");
    const cacheSQL = fs.readFileSync(cacheFilePath).toString();

    const companiesID = new TableID(
        "public",
        "companies",
    );
    const ordersID = new TableID(
        "public",
        "orders",
    );
    const vatsID = new TableID(
        "public",
        "vats",
    );
    const unitTypesID = new TableID(
        "public",
        "unit_type",
    );
    const invoiceID = new TableID(
        "public",
        "invoice",
    );
    const trainID = new TableID(
        "public",
        "train",
    );
    const gtdID = new TableID(
        "public",
        "list_gtd",
    );
    const operationID = new TableID(
        "operation",
        "operation",
    );

    const testDatabase = new Database([
        new Table(
            companiesID.schema,
            companiesID.name,
            [
                new Column(
                    companiesID,
                    "bigint_orders_ids",
                    "bigint[]"
                )
            ]
        ),
        new Table(
            ordersID.schema,
            ordersID.name,
            [
                new Column(
                    ordersID,
                    "companies_ids",
                    "integer[]"
                ),
                new Column(
                    ordersID,
                    "clients_ids",
                    "integer[]"
                ),
                new Column(
                    ordersID,
                    "partners_ids",
                    "integer[]"
                ),
                new Column(
                    ordersID,
                    "order_date",
                    "date"
                )
            ]
        ),
        new Table(
            vatsID.schema,
            vatsID.name,
            [
                new Column(
                    vatsID,
                    "vat_value",
                    "numeric"
                )
            ]
        ),
        new Table(
            unitTypesID.schema,
            unitTypesID.name,
            [
                new Column(
                    unitTypesID,
                    "id",
                    "integer"
                ),
                new Column(
                    unitTypesID,
                    "id_category",
                    "integer"
                ),
                new Column(
                    unitTypesID,
                    "name",
                    "text"
                )
            ]
        ),
        new Table(
            invoiceID.schema,
            invoiceID.name,
            [
                new Column(
                    invoiceID,
                    "id",
                    "integer"
                ),
                new Column(
                    invoiceID,
                    "renomination_invoices",
                    "int8[]"
                ),
                new Column(
                    invoiceID,
                    "payments_ids",
                    "int8[]"
                ),
                new Column(
                    invoiceID,
                    "orders_ids",
                    "bigint[]"
                )
            ]
        ),
        new Table(
            trainID.schema,
            trainID.name,
            [
                new Column(
                    trainID,
                    "id",
                    "integer"
                ),
                new Column(
                    trainID,
                    "units_ids",
                    "int8[]"
                )
            ]
        ),
        new Table(
            gtdID.schema,
            gtdID.name,
            [
                new Column(
                    gtdID,
                    "id",
                    "integer"
                ),
                new Column(
                    gtdID,
                    "orders_ids",
                    "bigint[]"
                )
            ]
        ),
        new Table(
            operationID.schema,
            operationID.name,
            [
                new Column(
                    gtdID,
                    "id",
                    "integer"
                ),
                new Column(
                    gtdID,
                    "doc_parent_id_order",
                    "bigint"
                ),
                new Column(
                    gtdID,
                    "id_order",
                    "bigint"
                )
            ]
        )
    ]);

    const builder = new CacheTriggersBuilder(
        cacheSQL,
        testDatabase
    );
    const triggers = builder.createTriggers();

    for (let schemaTable of test.tables) {
        const triggerFilePath = path.join(test.testDir, schemaTable + ".sql");
        const expectedTriggerDDL = fs.readFileSync(triggerFilePath).toString();

        schemaTable = schemaTable.split(".").slice(0, 2).join(".");

        const output = triggers.find(trigger => 
            expectedTriggerDDL.includes(trigger.name)
        );
        assert.ok(output, "should be trigger for table: " + schemaTable);

        const actualTriggerDDL = (
            output.function.toSQL() + 
            ";\n\n" + 
            output.trigger.toSQL() +
            ";"
        );

        assert.strictEqual(
            actualTriggerDDL,
            expectedTriggerDDL
        )
    }
}
