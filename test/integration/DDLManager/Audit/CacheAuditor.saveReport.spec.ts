import { Pool } from "pg";
import fs from "fs";
import {prepare, buildDDL} from "./fixture";
import { CacheAuditor, CacheScanner } from "../../../../lib/Auditor";
import { FileReader } from "../../../../lib/fs/FileReader";
import { Database } from "../../../../lib/database/schema/Database";
import { PostgresDriver } from "../../../../lib/database/PostgresDriver";
import { CacheColumnGraph } from "../../../../lib/Comparator/graph/CacheColumnGraph";
import { deepEqualRow, shouldBeBetween } from "./utils";
import { strict } from "assert";

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("CacheAuditor: fix data and save report", () => {
    
    let db: Pool;
    beforeEach(async () => {
        db = await prepare();
    });

    describe("audit simple broken cache", () => {
        beforeEach(async() => {
            fs.writeFileSync(ROOT_TMP_PATH + "/cache.sql", `
                cache totals for companies (
                    select
                        1 as some_column
                )
            `);
            await build();

            await db.query(`
                update companies set
                    some_column = 0
            `);
        });

        it("should fix data", async () => {
            await audit();
            
            const result = await db.query(`
                select id, some_column 
                from companies
                order by id
            `);
            strict.deepEqual(result.rows, [
                {id: 1, some_column: 1},
                {id: 2, some_column: 1}
            ]);
        });

        it("need recreate report columns", async() => {
            await db.query(`
                create table ddl_manager_audit_report (
                    id bigserial primary key
                );
                update companies set 
                    some_column = 0;
            `);

            await audit();

            await expectedReport("cache_column", "public.companies.some_column");
        });

        describe("should save report", async () => {

            it("save schema, table, column", async() => {
                await audit();

                await expectedReport("cache_column", "public.companies.some_column");
            });

            it("save expected, actual cache value", async() => {
                await audit();

                await expectedReport("expected_value", 1);
                await expectedReport("actual_value", 0);
            });
            
            it("save scan date", async() => {
                const dateStart = new Date();
                await audit();
                const dateEnd = new Date();

                const lastReport = await loadLastReport();
                shouldBeBetween(lastReport.scan_date, dateStart, dateEnd);
            });
            
            it("save cache row", async() => {
                await audit();

                const lastReport = await loadLastReport();
                deepEqualRow(lastReport.cache_row, {
                    id: 1,
                    name: "client",
                    some_column: 0
                });
            });
        });

    });

    async function build() {
        await buildDDL(db);
    }

    async function audit() {
        const fsState = FileReader.read([ROOT_TMP_PATH]);
        const dbState = new Database();
        const graph = CacheColumnGraph.build(
            new Database().aggregators,
            fsState.allCache()
        );
        const postgres = new PostgresDriver(db);

        const scanner = new CacheScanner(
            postgres,
            dbState,
            graph
        );
        const auditor = new CacheAuditor(
            postgres,
            dbState,
            fsState,
            graph,
            scanner,
        );

        await auditor.audit();
    }

    async function expectedReport(column: string, expected: any) {
        const lastReport = await loadLastReport();

        strict.ok(lastReport, "report should be saved");
        strict.deepEqual({
            [column]: lastReport[column]
        }, {
            [column]: expected
        });
    }

    async function loadLastReport() {
        const {rows} = await db.query(`
            select
                *,
                to_char(
                    ddl_manager_audit_report.scan_date at time zone 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
                ) as scan_date
            from ddl_manager_audit_report
            order by ddl_manager_audit_report.id desc
            limit 1
        `);
        return rows[0];
    }

})