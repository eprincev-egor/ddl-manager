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
        await this.createTable();
        await this.scanner.scan({
            onScanColumn: this.onScanColumn.bind(this)
        });
    }

    private async createTable() {
        await this.driver.query(`
            create table ddl_manager_audit_report (
                id serial primary key,
                scan_date timestamp without time zone,
                cache_name text not null,
                schema_name text not null,
                table_name text not null,
                column_name text not null,
                cache_row json not null,
                source_rows json,
                actual_value json,
                expected_value json
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
    }

    private async saveReport(event: IColumnScanResult) {
        const wrongExample = event.wrongExample;
        strict.ok(wrongExample, "required wrongExample");

        const cacheColumn = this.parseColumn(event);

        await this.driver.query(`
            insert into ddl_manager_audit_report (
                scan_date,
                cache_name,
                schema_name,
                table_name,
                column_name,
                cache_row,
                source_rows,
                actual_value,
                expected_value
            ) values (
                now()::timestamp with time zone at time zone 'UTC',
                ${wrapText(cacheColumn.cache.name)},
                ${wrapText(cacheColumn.getSchemaName())},
                ${wrapText(cacheColumn.getTableName())},
                ${wrapText(cacheColumn.name)},
                ${wrapText(JSON.stringify(wrongExample.row))}::json,
                ${wrapText(JSON.stringify(wrongExample.sourceRows ?? null))}::json,
                ${wrapText(JSON.stringify(wrongExample.actual))}::json,
                ${wrapText(JSON.stringify(wrongExample.expected))}::json
            )
        `);
    }

    private async fixData(event: IColumnScanResult) {
        const cacheColumn = this.parseColumn(event);

        const updates = CacheUpdate.fromManyTables([cacheColumn])

        await UpdateMigrator.migrate(
            this.driver,
            this.database,
            updates,
        );
    }

    private parseColumn(event: IColumnScanResult) {
        const [schemaName, tableName, columnName] = event.column.split(".");
        const tableId = schemaName + "." + tableName;

        const cacheColumn = this.graph.getColumn(tableId, columnName);
        strict.ok(cacheColumn, `unknown column: ${event.column}`);

        return cacheColumn;
    }
}