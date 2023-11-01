import { CacheColumnGraph } from "../Comparator/graph/CacheColumnGraph";
import { IDatabaseDriver } from "../database/interface";
import { CacheScanner, IColumnScanResult } from "./CacheScanner";
import { UpdateMigrator } from "../Migrator/UpdateMigrator";
import { Database } from "../database/schema/Database";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { wrapText } from "../database/postgres/wrapText";
import { FilesState } from "../fs/FilesState";
import { flatMap, uniq } from "lodash";
import { TableID } from "../database/schema/TableID";
import { FileParser } from "../parser";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { strict } from "assert";

export interface IAuditParams {
    timeout?: number;
    concreteTables?: string | string[];
}

export class CacheAuditor {

    constructor(
        private driver: IDatabaseDriver,
        private database: Database,
        private fs: FilesState,
        private graph: CacheColumnGraph,
        private scanner: CacheScanner
    ) {}

    async audit(params: IAuditParams = {}) {
        await this.createTables();
        await this.removeUnnecessaryLoggers();

        await this.scanner.scan({
            ...params,
            onScanColumn: async (event) => {
                await this.onScanColumn(event).catch(error =>
                    console.log("failed handler onScanColumn", error)
                );
            }
        });
    }

    private async createTables() {
        await this.createReportTable();
        await this.createChangesTable();
    }

    private async createReportTable() {
        await this.driver.query(`
            create table if not exists ddl_manager_audit_report (
                id serial primary key
            );
            alter table ddl_manager_audit_report

                add column if not exists scan_date timestamp without time zone,
                    alter column scan_date set not null,

                add column if not exists cache_column text,
                    alter column cache_column set not null,

                add column if not exists cache_row json,
                    alter column cache_row set not null,

                add column if not exists source_rows json,
                add column if not exists actual_value json,
                add column if not exists expected_value json;
            
            create index if not exists ddl_manager_audit_report_column_idx
            on ddl_manager_audit_report
            using btree (cache_column);
            
            create index if not exists ddl_manager_audit_report_cache_row_idx
            on ddl_manager_audit_report
            using btree (((cache_row->'id')::text::bigint));
        `);
    }

    private async createChangesTable() {
        await this.driver.query(`
            create table if not exists ddl_manager_audit_column_changes (
                id bigserial primary key
            );

            alter table ddl_manager_audit_column_changes

                add column if not exists cache_columns text[],

                add column if not exists changed_table text,
                    alter column changed_table set not null,

                add column if not exists changed_row_id bigint,
                    alter column changed_row_id set not null,

                add column if not exists changed_old_row json,
                add column if not exists changed_new_row json,

                add column if not exists changes_type text,
                    alter column changes_type set not null,

                add column if not exists transaction_date timestamp without time zone,
                    alter column transaction_date set not null,

                add column if not exists changes_date timestamp without time zone,
                    alter column changes_date set not null,

                add column if not exists entry_query text not null,
                    alter column entry_query set not null,

                add column if not exists callstack text[];

            create index if not exists ddl_manager_audit_column_changes_cache_columns_idx
            on ddl_manager_audit_column_changes
            using gin (cache_columns);

            create index if not exists ddl_manager_audit_column_changes_row_idx
            on ddl_manager_audit_column_changes
            using btree (changed_table, changed_row_id);
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
        const cache = required(this.fs.getCache( cacheColumn.cache.signature ));

        if ( cache.hasForeignTablesDeps() ) {
            const fromTable = cache.select.getFromTableId();
            await this.buildLoggerFor(fromTable);

            if ( !fromTable.equal(cache.for.table) ) {
                await this.buildLoggerFor(cache.for.table);
            }
        }
        else {
            await this.buildLoggerFor(cache.for.table);
        }
    }

    private async buildLoggerFor(table: TableID) {
        const depsCacheColumns = this.graph.findCacheColumnsDependentOn(table);

        const tableColumns = flatMap(depsCacheColumns, 
            column => column.select.getAllColumnReferences()
        )
            .filter(columnRef => columnRef.isFromTable(table))
            .map(column => column.name)
            .sort();

        const depsCacheColumnsOnTriggerTable = depsCacheColumns.filter(someCacheColumn =>
            someCacheColumn.for.table.equal(table)
        ).map(column => column.name);
        tableColumns.push(...depsCacheColumnsOnTriggerTable);

        tableColumns.push("id");

        await this.buildLoggerOn({
            triggerTable: table,
            triggerTableColumns: uniq(tableColumns),
            cacheColumns: depsCacheColumns
                .map(column => column.getId())
        });
    }

    private async buildLoggerOn({
        triggerTable, triggerTableColumns,
        cacheColumns,
    }: {
        triggerTable: TableID,
        cacheColumns: string[],
        triggerTableColumns: string[]
    }) {
        const triggerName = `ddl_audit_changes_${triggerTable.schema}_${triggerTable.name}`;
        const noChanges = triggerTableColumns.map(column =>
            `new.${column} is not distinct from old.${column}`
        ).join(" and ");

        await this.driver.query(`
            create or replace function ${triggerName}()
            returns trigger as $body$
            declare callstack text;
            declare this_row record;
            begin
                this_row = case when TG_OP = 'DELETE' then old else new end;

                if TG_OP = 'UPDATE' then
                    if ${noChanges} then
                        return new;
                    end if;
                end if;

                -- ignore update-ddl-cache
                if current_query() ~* '-- cache' then
                    return this_row;
                end if;

                begin
                    raise exception 'callstack';
                exception when others then
                get stacked diagnostics
                    callstack = PG_EXCEPTION_CONTEXT;
                end;

                insert into ddl_manager_audit_column_changes (
                    cache_columns,

                    changed_table,
                    changed_row_id,
                    changed_old_row,
                    changed_new_row,

                    changes_type,
                    transaction_date,
                    changes_date,

                    entry_query,
                    callstack
                ) values (
                    ARRAY[${cacheColumns.map(name => wrapText(name))}]::text[],

                    '${triggerTable}',
                    this_row.id,
                    case when TG_OP in ('UPDATE', 'DELETE') then
                        ${pickJson("old", triggerTableColumns)}
                    end,
                    case when TG_OP in ('UPDATE', 'INSERT') then
                        ${pickJson("new", triggerTableColumns)}
                    end,
                    
                    TG_OP,
                    transaction_timestamp()::timestamp with time zone at time zone 'UTC',
                    clock_timestamp()::timestamp with time zone at time zone 'UTC',

                    current_query(),
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
                            line !~* '${triggerName}' 
                            and
                            line ~* '(function|функция) '
                    )
                );

                return this_row;
            end
            $body$ language plpgsql;

            drop trigger if exists zzzzz_${triggerName} on ${triggerTable};
            create trigger zzzzz_${triggerName}
            after insert or update or delete
            on ${triggerTable}
            for each row
            execute procedure ${triggerName}();

            comment on trigger zzzzz_${triggerName} on ${triggerTable}
            is 'ddl-manager-audit';
        `);
    }

    private findCacheColumn(event: IColumnScanResult) {
        const [schemaName, tableName, columnName] = event.column.split(".");
        const tableId = schemaName + "." + tableName;

        const cacheColumn = this.graph.getColumn(tableId, columnName);
        strict.ok(cacheColumn, `unknown column: ${event.column}`);

        return cacheColumn;
    }

    private async removeUnnecessaryLoggers() {
        const {rows: triggers} = await this.driver.query(`
            select
                pg_get_triggerdef( pg_trigger.oid ) as ddl
            from pg_trigger
            where
                pg_trigger.tgisinternal = false and
                pg_catalog.obj_description( pg_trigger.oid ) = 'ddl-manager-audit'
        `);
        for (const {ddl} of triggers) {
            const parsed = FileParser.parse(ddl);
            const [trigger] = parsed.triggers;

            await this.removeLoggerTriggerIfUnnecessary(trigger);
        }
    }

    private async removeLoggerTriggerIfUnnecessary(trigger: DatabaseTrigger) {
        const needDrop = await this.isUnnecessaryLoggerTrigger(trigger);
        if ( needDrop ) {
            await this.driver.dropTrigger(trigger);
        }
    }
    
    private async isUnnecessaryLoggerTrigger(trigger: DatabaseTrigger) {
        const existentCacheColumns = this.graph.findCacheColumnsDependentOn(trigger.table);
        if ( existentCacheColumns.length === 0 ) {
            return true;
        }

        const cacheColumns = existentCacheColumns
            .map(column => column.getId());

        const {rows: reports} = await this.driver.query(`
            select * from ddl_manager_audit_report as last_report
            where
                last_report.cache_column in (${
                    cacheColumns.map(column => wrapText(column))
                        .join(", ")
                })

            order by last_report.scan_date desc
            limit 1
        `);
        const lastReport = reports[0];
        const MONTH = 30 * 24 * 60 * 60 * 1000;
        const isOld = Date.now() - lastReport.scan_date > 3 * MONTH;
        return isOld
    }
}

function pickJson(row: string, columns: string[]) {
    return `json_build_object(
        ${columns.map(column => `'${column}', ${row}.${column}`)}
    )`.trim();
}

function required<T>(value: T): NonNullable<T> {
    strict.ok(value, "required value");
    return value as any;
}

// TODO: cache columns can be changed (change cache file)
// TODO: test timeout
// TODO: log all changes columns inside incident (can be helpful to find source of bug)