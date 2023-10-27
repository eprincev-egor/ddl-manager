import { CacheColumnGraph } from "../Comparator/graph/CacheColumnGraph";
import { IDatabaseDriver } from "../database/interface";
import { CacheScanner, IColumnScanResult } from "./CacheScanner";
import { UpdateMigrator } from "../Migrator/UpdateMigrator";
import { Database } from "../database/schema/Database";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { wrapText } from "../database/postgres/wrapText";
import { buildReferenceMeta } from "../cache/processor/buildReferenceMeta";
import { FilesState } from "../fs/FilesState";
import { strict } from "assert";
import { flatMap, uniq } from "lodash";

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
        await this.scanner.scan({
            ...params,
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

                cache_columns text[],
                cache_rows_ids text[],

                changed_table text not null,
                changed_row_id bigint not null,
                changed_old_row json,
                changed_new_row json,

                changes_type text not null,
                transaction_date timestamp without time zone not null,
                changes_date timestamp without time zone not null,

                entry_query text not null,
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
        const cache = this.fs.getCache( cacheColumn.cache.signature );
        const wrongExample = event.wrongExample;
        strict.ok(wrongExample && cacheColumn && cache, "something wrong");


        const fromTable = cache.hasForeignTablesDeps() ?  // TODO: polymorphism
            cache.select.getFromTableId() :
            cache.for.table;
        const depsCacheColumns = this.graph.findCacheColumnsDependentOn(fromTable);

        let fromTableColumns = flatMap(depsCacheColumns, column => 
            column.select.getAllColumnReferences()
        ).filter(columnRef => columnRef.isFromTable(fromTable))
        .map(column => column.name)
        .sort();

        if ( !cache.hasForeignTablesDeps() ) {
            fromTableColumns.push(cacheColumn.name);
        }
        fromTableColumns = uniq(fromTableColumns);

    
        const depsCacheColumnsIds = depsCacheColumns.map(column => column.getId());

        const reference = buildReferenceMeta(cache, fromTable);
        const referenceColumns = cache.hasForeignTablesDeps() ? 
            reference.columns : ["id"];
        const thisRowReferenceColumns = referenceColumns.map(column =>
            `this_row.${column}`
        );

        await this.driver.query(`
            create or replace function ddl_manager_audit_changes_listener()
            returns trigger as $body$
            declare callstack text;
            declare this_row record;
            begin
                this_row = case when TG_OP = 'DELETE' then old else new end;

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
                    cache_rows_ids,

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
                    ARRAY[${depsCacheColumnsIds.map(name => wrapText(name))}],
                    ARRAY[${thisRowReferenceColumns}],
                    
                    '${fromTable}',
                    this_row.id,
                    case when TG_OP in ('UPDATE', 'DELETE') then
                        ${pickJson("old", fromTableColumns)}
                    end,
                    case when TG_OP in ('UPDATE', 'INSERT') then
                        ${pickJson("new", fromTableColumns)}
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
                            line !~* 'ddl_manager_audit_changes_listener' 
                            and
                            line ~* '(function|функция) '
                    )
                );

                return this_row;
            end
            $body$ language plpgsql;

            create trigger zzzzz_ddl_manager_audit_changes_listener
            after insert or update of ${fromTableColumns} or delete
            on ${fromTable}
            for each row
            execute procedure ddl_manager_audit_changes_listener();
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

function pickJson(row: string, columns: string[]) {
    return `json_build_object(
        ${columns.map(column => `'${column}', ${row}.${column}`)}
    )`.trim();
}

// TODO: remove changes listener
// TODO: cache columns can be changed (change cache file)
// TODO: cache columns can be dropped
// TODO: test many broken columns on one table
// TODO: test cache with array ref
// TODO: log all changes columns inside incident (can be helpful to find source of bug)
// TODO: test recreating table
    // TODO: indexes
    // TODO: dont loose old data
// TODO: test timeout