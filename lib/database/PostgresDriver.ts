import fs from "fs";
import { Pool } from "pg";
import { IDatabaseDriver, MinMax } from "./interface";
import { FileParser } from "../parser";
import { PGTypes } from "./PGTypes";
import { Select } from "../ast";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { Comment } from "./schema/Comment";
import { DatabaseFunction } from "./schema/DatabaseFunction";
import { DatabaseTrigger } from "./schema/DatabaseTrigger";
import { TableReference } from "./schema/TableReference";
import { Table } from "./schema/Table";
import { TableID } from "./schema/TableID";
import { flatMap } from "lodash";
import { wrapText } from "./postgres/wrapText";
import { IFileContent } from "../fs/File";
import { Index } from "./schema/Index";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";

const selectAllFunctionsSQL = fs.readFileSync(__dirname + "/postgres/select-all-functions.sql")
    .toString();
const selectAllTriggersSQL = fs.readFileSync(__dirname + "/postgres/select-all-triggers.sql")
    .toString();
const selectAllColumnsSQL = fs.readFileSync(__dirname + "/postgres/select-all-columns.sql")
    .toString();
const selectAllAggregatorsSQL = fs.readFileSync(__dirname + "/postgres/select-all-aggregators.sql")
    .toString();
const selectAllIndexesSQL = fs.readFileSync(__dirname + "/postgres/select-all-indexes.sql")
    .toString();

export class PostgresDriver
implements IDatabaseDriver {

    private pgPool: Pool;
    private fileParser: FileParser;
    private types: PGTypes;

    constructor(pgClient: Pool) {
        this.pgPool = pgClient;
        this.fileParser = new FileParser();
        this.types = new PGTypes(pgClient);
    }

    async load() {
        const database = await this.loadTables();
        
        const functions = await this.loadFunctions();
        database.addFunctions(functions);

        const triggers = await this.loadTriggers();
        for (const trigger of triggers) {
            database.addTrigger(trigger);
        }
        const indexes = await this.loadIndexes();
        for (const index of indexes) {
            database.addIndex(index);
        }

        const aggregators = await this.loadAggregators();
        database.addAggregators(aggregators);

        return database;
    }

    async enableTrigger(onTable: TableID, triggerName: string): Promise<void> {
        await this.query(`
            alter table ${onTable}
            enable trigger ${triggerName};
        `);
    }

    async disableTrigger(onTable: TableID, triggerName: string): Promise<void> {
        await this.query(`
            alter table ${onTable}
            disable trigger ${triggerName};
        `);
    }

    private async loadFunctions(): Promise<DatabaseFunction[]> {
        const funcs = await this.loadObjects<DatabaseFunction>(
            selectAllFunctionsSQL
        );
        return funcs;
    }

    private async loadTriggers(): Promise<DatabaseTrigger[]> {
        const triggers = await this.loadObjects<DatabaseTrigger>(
            selectAllTriggersSQL
        );
        return triggers;
    }

    private async loadIndexes(): Promise<Index[]> {
        const {rows} = await this.query(selectAllIndexesSQL);
        const indexes = rows.map(row => {

            const usingMatch = row.indexdef.match(/using\s+(\w+)\s+\(/i) || [];
            const indexType = usingMatch[1] || "unknown";

            // "lala USING btree ( col, (expr), ... )"
            // =>
            // " col, (expr), ... "
            const columnsStr = row.indexdef
                .split("(").slice(1).join("(")
                .split(")").slice(0, -1).join(")");
            const columns = FileParser.parseIndexColumns(columnsStr);

            const index = new Index({
                name: row.indexname,
                table: new TableID(
                    row.schemaname,
                    row.tablename
                ),
                index: indexType,
                columns,
                comment: Comment.fromTotalString(
                    "index",
                    row.comment
                )
            });

            return index;
        });

        return indexes;
    }

    private async loadObjects<T>(selectAllObjectsSQL: string): Promise<T[]> {
        const objects: any[] = [];

        const {rows} = await this.query(selectAllObjectsSQL);
        for (const row of rows) {

            const fileContent = this.fileParser.parse(row.ddl) as IFileContent;
            
            const funcJson = fileContent.functions[0];
            if ( funcJson ) {
                const func = new DatabaseFunction({
                    ...funcJson,
                    comment: Comment.fromTotalString( "function", row.comment )
                });
                objects.push(func);
            }

            const triggerJson = fileContent.triggers[0];
            if ( triggerJson ) {
                const trigger = new DatabaseTrigger({
                    ...triggerJson,
                    comment: Comment.fromTotalString( "trigger", row.comment )
                });
                objects.push(trigger);
            }
        }

        return objects;
    }

    private async loadTables() {
        await this.types.load();

        const {rows: columnsRows} = await this.query(selectAllColumnsSQL);
        
        const database = new Database();
        for (const columnRow of columnsRows) {
            
            const tableId = new TableID(
                columnRow.table_schema,
                columnRow.table_name
            );
            let table = database.getTable(tableId);
            if ( !table ) {
                database.setTable(new Table(
                    columnRow.table_schema,
                    columnRow.table_name
                ));
                table = database.getTable(tableId)!;
            }

            const columnType = this.types.getTypeById(
                columnRow.column_type_oid
            ) as string;
            const column = new Column(
                tableId,
                columnRow.column_name,
                columnType,
                columnRow.column_default,
                Comment.fromTotalString( "column", columnRow.comment )
            );
            // nulls: parseColumnNulls(columnRow)
            table.addColumn(column);
        }

        return database;
    }

    private async loadAggregators(): Promise<string[]> {
        const {rows} = await this.query(selectAllAggregatorsSQL);
        const aggregators = rows.map(row => row.agg_func_name) as string[];
        return aggregators;
    }

    async unfreezeAll(dbState: Database) {
        let ddlSql = "";

        dbState.functions.forEach(func => {
            ddlSql += getUnfreezeFunctionSql( func );
            ddlSql += ";";
        });

        flatMap(dbState.tables, table => table.triggers).forEach(trigger => {
            ddlSql += getUnfreezeTriggerSql( trigger );
            ddlSql += ";";
        });

        await this.query(ddlSql);
    }

    async createOrReplaceFunction(func: DatabaseFunction) {
        try {
            // if func has other arguments,
            // then need drop function before replace
            // 
            // but can exists triggers or views who dependent on this function
            await this.dropFunction(func);
        } catch(err) {
            // 
        }

        let ddlSql = func.toSQL();

        if ( !func.comment.isEmpty() ) {
            ddlSql += `;\ncomment on function ${func.getSignature()} is ${wrapText(func.comment.toString())}`
        }

        await this.query(ddlSql);
    }

    async createOrReplaceLogFunction(func: DatabaseFunction) {
        let ddlSql = func.toSQLWithLog();

        if ( !func.comment.isEmpty() ) {
            ddlSql += `;\ncomment on function ${func.getSignature()} is ${wrapText(func.comment.toString())}`
        }

        await this.query(ddlSql);
    }

    async dropFunction(func: DatabaseFunction) {
        const sql = `drop function if exists ${ func.getSignature() }`;
        await this.query(sql);
    }

    async createOrReplaceTrigger(trigger: DatabaseTrigger) {
        let ddlSql = `drop trigger if exists ${ trigger.getSignature() };\n`;
        
        ddlSql += trigger.toSQL();

        if ( !trigger.comment.isEmpty() ) {
            ddlSql += `;\ncomment on trigger ${trigger.getSignature()} is ${wrapText(trigger.comment.toString())}`
        }

        await this.query(ddlSql);
    }

    async dropTrigger(trigger: DatabaseTrigger) {
        const sql = `drop trigger if exists ${ trigger.getSignature() }`;
        await this.query(sql);
    }

    async getCacheColumnsTypes(select: Select, forTable: TableReference) {
        await this.types.load();

        const sql = `
            select
                ddl_manager_dmp.*
            from ${forTable.toString()}

            left join lateral (
                ${ select }
                where false
            ) as ddl_manager_dmp on true

            where false
        `;
        const {fields} = await this.query(sql);

        const columnsTypes: {[columnName: string]: string} = {};
        for (const field of fields) {
            const typeId = field.dataTypeID;
            const type = this.types.getTypeById(typeId) as string;

            columnsTypes[ field.name ] = type;
        }
        return columnsTypes;
    }

    async createOrReplaceColumn(column: Column) {
        let sql = `
            alter table ${column.table} add column if not exists ${column.name} ${column.type} default ${ column.default };
        `;
        if ( !column.comment.isEmpty() ) {
            sql += `comment on column ${ column.getSignature() } is ${wrapText( column.comment.toString() )}`;
        }

        await this.query(sql);
    }

    async dropColumn(column: Column) {
        const sql = `
            alter table ${column.table} drop column if exists ${column.name};
        `;

        await this.query(sql);
    }

    async selectMinMax(table: TableID): Promise<MinMax> {
        // need sort by asc, because new rows may be created
        const sql = `
            select
                min(id) as min,
                max(id) as max
            from ${table}
        `;
        const {rows} = await this.query(sql);
        return {
            min: toNumber(rows[0].min),
            max: toNumber(rows[0].max)
        }
    }

    async updateCacheForRows(
        update: CacheUpdate,
        minId: number,
        maxId: number
    ) {
        const sql = this.buildUpdateSql(update, `
            and
            ${update.table.getIdentifier()}.id >= ${minId} and
            ${update.table.getIdentifier()}.id < ${maxId}
        `);
        await this.query(sql);
    }

    async updateCacheLimitedPackage(
        update: CacheUpdate,
        limit: number
    ) {
        const sql = this.buildUpdateSql(update, `
            order by ${update.table.getIdentifier()}.id asc
            limit ${ limit }
        `) + `\n returning ${update.table.getIdentifier()}.id`;
        const {rows} = await this.query(sql);
        return rows.length;
    }
    
    private buildUpdateSql(
        update: CacheUpdate,
        filter: string
    ) {
        const sets: string[] = [];
        const whereRowIsBroken: string[] = [];
        const joins: string[] = [];
        const joinsNames: string[] = [];

        for (let i = 0, n = update.selects.length; i < n; i++) {
            const select = update.selects[i];
            const joinName = `expected_${i}`;

            joinsNames.push(joinName);

            for (const column of select.columns) {
                sets.push(`${column.name} = ddl_manager_tmp.${column.name}`);
                whereRowIsBroken.push(
                    `${update.table.getIdentifier()}.${column.name} 
                        is distinct from 
                    ${joinName}.${column.name}`
                );
            }

            joins.push(`
                left join lateral (
                    ${select}
                ) as ${joinName} on true
            `);
        }

        const selectBrokenRows = `
            select
                ${update.table.getIdentifier()}.id,
                ${joinsNames.map(joinName => joinName + ".*").join(",\n")}
            from ${update.table.toString()}
            
            ${joins.join("\n\n")}

            where
                (${ whereRowIsBroken.join("\nor\n") })
                ${filter}
        `;

        const sql = `-- cache ${update.caches.join(", ")} for ${update.table.table}
            with ddl_manager_tmp as (
                ${selectBrokenRows}
            )
            update ${update.table.toString()} set
                ${sets.join(", ")}
            from ddl_manager_tmp
            where
                ddl_manager_tmp.id = ${update.table.getIdentifier()}.id
        `;
        return sql;
    }

    async createOrReplaceHelperFunc(func: DatabaseFunction) {
        const sql = `
            drop function if exists ${ func.getSignature() };

            ${ func.toSQL() };

            comment on function ${ func.getSignature() }
            is 'ddl-manager-helper';
        `;

        await this.query(sql);
    }

    async dropIndex(index: Index) {
        const sql = `
            drop index if exists ${ index.getSignature() };
        `;
        await this.query(sql);

    }

    async createOrReplaceIndex(index: Index) {
        const sql = `
            drop index if exists ${ index.getSignature() };

            ${ index.toSQL() };

            comment on index ${ index.getSignature() }
            is ${ wrapText( index.comment.toString() ) };
        `;

        await this.query(sql);
    }

    async end() {
        await this.pgPool.end();
    }

    private async query(sql: string) {
        try {
            return await this.pgPool.query(sql);
        } catch(originalErr) {
            // redefine call stack
            const {message, code} = originalErr as any;
            const err = new Error(message);
            (err as any).sql = sql;
            (err as any).code = code;
            (err as any).originalError = originalErr;
            console.error(originalErr);
            console.error(sql);
            throw err;
        }
    }
}

function toNumber(value: string | number | null | undefined): number | null {
    if ( value == null ) {
        return null;
    }
    return +value;
}

// function parseColumnNulls(columnRow: any) {
//     if ( columnRow.is_nullable === "YES" ) {
//         return true;
//     } else {
//         return false;
//     }
// }