import fs from "fs";
import { Pool, PoolClient, QueryResult } from "pg";
import { IDatabaseDriver, MinMax } from "./interface";
import { FileParser } from "../parser";
import { PGTypes } from "./PGTypes";
import { Expression, Select } from "../ast";
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
import { parseIndexColumns } from "../parser/parseIndexColumns";
import { fixErrorStack } from "../utils";

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
    private reservedConnection?: PoolClient;
    private reservingConnection?: Promise<any>;

    constructor(pgPool: Pool) {
        this.pgPool = pgPool;
        this.pgPool.on("error", error =>
            console.error("got pg pool error", error)
        );

        // hardfix falling process
        (this.pgPool as any)._releaseOnce = function _releaseOnce(client: any, idleListener: any) {
            let released = false
        
            return (err: Error) => {
                if (released) {
                    return;
                }
        
                released = true
                this._release(client, idleListener, err)
            };
        }

        this.fileParser = new FileParser();
        this.types = new PGTypes(pgPool);
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
            const columns = parseIndexColumns(columnsStr);

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

            const fileContent = this.fileParser.parseSql(row.ddl) as IFileContent;
            
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
                const rawComment = row.comment || "";
                const [comment, originalWhen] = rawComment.split("\noriginal-when: ");

                const trigger = new DatabaseTrigger({
                    ...triggerJson,
                    comment: Comment.fromTotalString( "trigger", comment ),
                    when: originalWhen || triggerJson.when
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
                columnRow.is_nullable === "YES",
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
            let comment = trigger.comment.toString();
            if ( trigger.when ) {
                comment += "\noriginal-when: ";
                comment += trigger.when;
            }
            
            ddlSql += `;
            comment on trigger ${trigger.getSignature()} 
            is ${wrapText(comment)}`
        }

        await this.query(ddlSql);
    }

    async dropTrigger(trigger: DatabaseTrigger) {
        const sql = `drop trigger if exists ${ trigger.getSignature() }`;
        await this.query(sql);
    }

    async getType(expression: Expression) {
        const sql = `
            select pg_typeof(${expression}) as type
        `;
        const {rows} = await this.query(sql);
        return rows[0].type;
    }

    async createOrReplaceColumn(column: Column) {
        let sql = `
            alter table ${column.table} 
                add column if not exists ${column.name} ${column.type}
                    default ${ column.default };

            comment on column ${ column.getSignature() } is ${
                column.comment.isEmpty() ? 
                    "null" : wrapText( column.comment.toString() )
            };
        `;

        if ( column.nulls ) {
            sql += `
            alter table ${column.table}
                alter column ${column.name}
                    drop not null;
            `;
        }
        else {
            sql += `
            do $$
            begin

            alter table ${column.table}
                alter column ${column.name}
                    set not null;

            exception when others then
            end $$;
            `;
        }
        sql += `
            alter table ${column.table}
                alter column ${column.name}
                    set default ${ column.default };

            do $$
            declare current_column_type text;
            declare trigger_name text;
            declare triggers_names text[];
            declare trigger_definition text;
            declare triggers_definitions text[];
            begin
                current_column_type = (
                    select to_regtype(column_info.udt_name)::text
                    from information_schema.columns as column_info
                    where
                        column_info.table_schema = '${column.table.schema}' and
                        column_info.table_name = '${column.table.name}' and
                        column_info.column_name = '${column.name}'
                );
                if current_column_type is distinct from '${column.type.normalized}' then

                    create or replace function ddl_manager__try_cast_to(
                        input_value text,
                        INOUT output_value ${column.type}
                    ) AS
                    $body$
                    begin
                        select cast( input_value as ${column.type} )
                        into output_value;
                        exception when others then
                    end
                    $body$ language plpgsql immutable;

                    select
                        array_agg( pg_trigger.tgname ),
                        array_agg( pg_get_triggerdef( pg_trigger.oid ) )
                    into
                        triggers_names
                        triggers_definitions
                    from pg_trigger
                    where
                        pg_trigger.tgisinternal = false and
                        pg_get_triggerdef( pg_trigger.oid ) ~* ' ON ${column.table} '
                    ;

                    if triggers_names is not null then
                        foreach trigger_name in array triggers_names loop
                            execute 'drop trigger if exists '  || trigger_name || ' on ${column.table};';
                        end loop;
                    end if;

                    alter table ${column.table}
                        alter column ${column.name}
                            set data type ${column.type}
                                using ddl_manager__try_cast_to(${column.name}::text, null::${column.type});

                    drop function ddl_manager__try_cast_to(text, ${column.type});

                    if triggers_definitions is not null then
                        foreach trigger_definition in array triggers_definitions loop
                            execute trigger_definition;
                        end loop;
                    end if;

                end if;
            end
            $$;
        `;

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

    async selectNextIds(
        table: TableID,
        maxId: number,
        limit: number
    ): Promise<number[]> {
        const {rows} = await this.query(`
            select id
            from ${table}
            where id < ${+maxId}
            order by id desc
            limit ${+limit}
        `);
        return rows.map(row => row.id).reverse();
    }

    async terminateActiveCacheUpdates() {
        await this.query(`
            select pg_cancel_backend(pid)
            from pg_stat_activity
            where
                query ilike '-- cache %' and
                query not ilike '%pg_cancel_backend(pid)%'
        `);
    }

    async updateCacheForRows(
        update: CacheUpdate,
        minId: number,
        maxId: number,
        timeout = 0
    ) {
        const sql = this.buildUpdateSql(update, `
            and
            ${update.table.getIdentifier()}.id >= ${minId} and
            ${update.table.getIdentifier()}.id <= ${maxId}
        `, `(${minId} - ${maxId})`);
        await this.queryWithTimeout(sql, timeout);
    }

    async updateCacheLimitedPackage(
        update: CacheUpdate,
        limit: number,
        timeout = 0
    ) {
        const sql = this.buildUpdateSql(update, `
            order by ${update.table.getIdentifier()}.id asc
            limit ${ limit }
        `) + `\n returning ${update.table.getIdentifier()}.id`;
        const {rows} = await this.queryWithTimeout(sql, timeout);
        return rows.map(row => +row.id);
    }
    
    private buildUpdateSql(
        update: CacheUpdate,
        filter: string,
        comment?: string
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

        const sql = `-- cache ${update.caches.join(", ")} for ${update.table.table} ${comment}
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
        try {
            this.reservedConnection?.release(); // can throw strange errors
        } catch {}
        try {
            await this.pgPool.end();
        } catch {}
    }

    async queryWithTimeout(sql: string, timeout = 0) {
        if ( timeout <= 0 ) {
            return await this.query(sql);
        }
        await this.getReservedConnection();


        const stack = new Error().stack!;
        let blocks;

        const connection = await this.getConnection();
        const processId = +(connection as any).processID;

        try {
            let wasReleased = false;

            const timer = setTimeout(async () => {
                const selectBlocks = selectBlocksQuery(processId);
                blocks = await this.queryInReservedConnection(selectBlocks)
                    .catch(error => [error]); // return error instead of blocks

                if ( wasReleased ) {
                    return;
                }

                await this.queryInReservedConnection(`
                    select pg_cancel_backend(${ processId })
                `);
            }, timeout);

            const result = await execSql(connection, sql).finally(() => {
                wasReleased = true;
                clearTimeout(timer);
                connection.release();
            });
            return result;
        } catch(originalErr) {
            (originalErr as any).blocks = blocks;
            throw fixErrorStack(sql, originalErr, stack);
        }
    }

    async query(sql: string) {
        const stack = new Error().stack;
        try {
            return await this.pgPool.query(sql);
        } catch(originalErr) {
            throw fixErrorStack(sql, originalErr, stack);
        }
    }

    private async queryInReservedConnection(sql: string) {
        const reservedConnection = await this.getReservedConnection();
        const {rows} = await execSql(reservedConnection, sql);
        return rows;
    }

    private async getReservedConnection() {
        if ( this.reservedConnection ) {
            return this.reservedConnection;
        }
        if ( this.reservingConnection ) {
            return await this.reservingConnection;
        }

        this.reservingConnection = this.getConnection();

        this.reservedConnection = await this.reservingConnection;
        return this.reservedConnection;
    }

    private async getConnection(): Promise<PoolClient> {
        const stack = new Error().stack!;
        const connection = await this.pgPool.connect()
            .catch(onGetConnectionError.bind(this, stack));
        
        // Fix falling the process
        connection.on("error", (error) =>
            console.log("got pg client error", error)
        );

        return connection;
    }
}

function selectBlocksQuery(processId: number) {
    return `
        SELECT blocking_locks.pid     AS blocking_pid,
                blocking_activity.query   AS blocking_query
        FROM  pg_catalog.pg_locks         blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks         blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid

            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE
            NOT blocked_locks.granted and
            blocked_locks.pid = ${+processId}
    `.trim();
}

function execSql(
    connection: PoolClient,
    sql: string
): Promise<QueryResult<any>> {
    return new Promise((resolve, reject) => {
        connection.query(sql, (err, result) => {
            if ( err ) {
                reject(err);
            }
            else {
                resolve(result);
            }
        });
    });
}

function onGetConnectionError(stack: string, error: Error): never {
    throw fixErrorStack("<getting connection>", error, stack);
}

function toNumber(value: string | number | null | undefined): number | null {
    if ( value == null ) {
        return null;
    }
    return +value;
}
