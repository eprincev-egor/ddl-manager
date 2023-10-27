import { CacheColumnGraph } from "../Comparator/graph/CacheColumnGraph";
import { IDatabaseDriver } from "../database/interface";
import { CacheScanner, IColumnScanResult } from "./CacheScanner";
import { UpdateMigrator } from "../Migrator/UpdateMigrator";
import { Database } from "../database/schema/Database";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { wrapText } from "../database/postgres/wrapText";
import { strict } from "assert";

export interface IAuditParams {
    timeout?: number;
    concreteTables?: string | string[];
}

export class CacheAuditor {

    constructor(
        private driver: IDatabaseDriver,
        private database: Database,
        private graph: CacheColumnGraph,
        private scanner: CacheScanner
    ) {}

    async audit(params: IAuditParams = {}) {
        await this.createTables();
        await this.scanner.scan({
            onScanColumn: this.onScanColumn.bind(this)
        });
    }

    private async createTables() {
        await this.createReportTable();
        await this.createChangesTable();
    }

    private async createReportTable() {
        await this.driver.query(`
            create table if not exists ddl_manager_audit_report (
                id serial primary key,
                scan_date timestamp without time zone,
                cache_column text not null,
                cache_row json not null,
                source_rows json,
                actual_value json,
                expected_value json
            );
        `);
    }

    private async createChangesTable() {
        await this.driver.query(`
            create table if not exists ddl_manager_audit_column_changes (
                id bigserial primary key,
                source_table text not null,
                source_row_id bigint not null,
                transaction_date timestamp without time zone not null,
                changes_date timestamp without time zone not null,
                callstack text[]
            );
        `);
    }

    private async onScanColumn(event: IColumnScanResult) {
        const wrongExample = event.wrongExample;
        if ( !wrongExample ) {
            return;
        }

        await this.saveReport(event);
        await this.fixData(event);
        await this.logChanges(event);
    }

    private async saveReport(event: IColumnScanResult) {
        const wrongExample = event.wrongExample;
        strict.ok(wrongExample, "required wrongExample");

        await this.driver.query(`
            insert into ddl_manager_audit_report (
                scan_date,
                cache_column,
                cache_row,
                source_rows,
                actual_value,
                expected_value
            ) values (
                now()::timestamp with time zone at time zone 'UTC',
                ${wrapText(event.column)},
                ${wrapText(JSON.stringify(wrongExample.cacheRow))}::json,
                ${wrapText(JSON.stringify(wrongExample.sourceRows ?? null))}::json,
                ${wrapText(JSON.stringify(wrongExample.actual))}::json,
                ${wrapText(JSON.stringify(wrongExample.expected))}::json
            )
        `);
    }

    private async fixData(event: IColumnScanResult) {
        const cacheColumn = this.findCacheColumn(event);

        const updates = CacheUpdate.fromManyTables([cacheColumn])

        await UpdateMigrator.migrate(
            this.driver,
            this.database,
            updates,
        );
    }

    private async logChanges(event: IColumnScanResult) {
        const cacheColumn = this.findCacheColumn(event);
        const wrongExample = event.wrongExample;
        strict.ok(wrongExample, "required wrongExample");

        const fromTable = cacheColumn.select.getFromTableId();

        await this.driver.query(`
            create or replace function ddl_manager_audit_changes_source_listener()
            returns trigger as $body$
            declare callstack text;            
            begin
                begin
                    raise exception 'callstack';
                exception when others then
                get stacked diagnostics
                    callstack = PG_EXCEPTION_CONTEXT;
                end;


                insert into ddl_manager_audit_column_changes (
                    source_table,
                    source_row_id,
                    transaction_date,
                    changes_date,
                    callstack
                ) values (
                    '${fromTable}',
                    new.id,
                    transaction_timestamp()::timestamp with time zone at time zone 'UTC',
                    clock_timestamp()::timestamp with time zone at time zone 'UTC',
                    (
                        select
                            array_agg(
                                (regexp_match(line, '(function|функция) ([\\w+.]+)'))[2] || ':' ||
                                (regexp_match(line, '(line|строка) (\\d+)'))[2]
                            )
                        from unnest(
                            regexp_split_to_array(callstack, '[\\n\\r]+')
                        ) as line
                        where
                            line !~* 'ddl_manager_audit_changes_source_listener' 
                            and
                            line ~* '(function|функция) '
                    )
                );

                return new;
            end
            $body$ language plpgsql;

            create trigger zzzzz_ddl_manager_audit_changes_source_listener
            after update
            on ${fromTable}
            for each row
            execute procedure ddl_manager_audit_changes_source_listener();
        `);
    }

    private findCacheColumn(event: IColumnScanResult) {
        const [schemaName, tableName, columnName] = event.column.split(".");
        const tableId = schemaName + "." + tableName;

        const cacheColumn = this.graph.getColumn(tableId, columnName);
        strict.ok(cacheColumn, `unknown column: ${event.column}`);

        return cacheColumn;
    }
}

// TODO: indexes
// TODO: remove changes listener
// TODO: cache columns can be changed (change cache file)
// TODO: cache columns can be dropped
// TODO: command update-ddl-cache can spawn a lot of changes
// TODO: test recreating table
// TODO: catch bugs with parallel transactions
// TODO: log all changes columns inside incident (can be helpful to find source of bug)
// TODO: test many broken columns on one table