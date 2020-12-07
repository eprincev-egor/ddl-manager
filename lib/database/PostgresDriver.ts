import fs from "fs";
import { Client } from "pg";
import { IDatabaseDriver } from "./interface";
import { FileParser } from "../parser";
import { PGTypes } from "./PGTypes";
import { Select } from "../ast";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { DatabaseFunction } from "./schema/DatabaseFunction";
import { DatabaseTrigger } from "./schema/DatabaseTrigger";
import { TableReference } from "./schema/TableReference";
import { Table } from "./schema/Table";
import { TableID } from "./schema/TableID";
import { flatMap } from "lodash";
import { wrapText } from "./postgres/wrapText";

const selectAllFunctionsSQL = fs.readFileSync(__dirname + "/postgres/select-all-functions.sql")
    .toString();
const selectAllTriggersSQL = fs.readFileSync(__dirname + "/postgres/select-all-triggers.sql")
    .toString();
const selectAllColumnsSQL = fs.readFileSync(__dirname + "/postgres/select-all-columns.sql")
    .toString();

export class PostgresDriver
implements IDatabaseDriver {

    private pgClient: Client;
    private fileParser: FileParser;
    private types: PGTypes;

    constructor(pgClient: Client) {
        this.pgClient = pgClient;
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

        return database;
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

    private async loadObjects<T>(selectAllObjectsSQL: string): Promise<T[]> {
        const objects: any[] = [];

        const {rows} = await this.query(selectAllObjectsSQL);
        for (const row of rows) {

            const fileContent = this.fileParser.parse(row.ddl) as any;
            const json = fileContent.functions[0] || fileContent.triggers[0];
 
            json.frozen = isFrozen(row);
            json.comment = parseComment(row);
            json.cacheSignature = parseCacheSignature(row);
        
            objects.push(json);
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
            const table = database.getTable(tableId) || new Table(
                columnRow.table_schema,
                columnRow.table_name
            );
            database.setTable(table);
            
            const columnType = this.types.getTypeById(
                columnRow.column_type_oid
            ) as string;
            const column = new Column(
                tableId,
                columnRow.column_name,
                columnType,
                columnRow.column_default,
                undefined,
                parseCacheSignature(columnRow)
            );
            // nulls: parseColumnNulls(columnRow)
            table.addColumn(column);
        }

        return database;
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
        
        ddlSql += ";";

        // TODO: move this code to Comparator
        let dbComment = "ddl-manager-sync";
        if ( func.comment ) {
            dbComment = func.comment + "\n" + "ddl-manager-sync";
        }
        if ( func.cacheSignature ) {
            dbComment += ` ddl-cache-signature(${ func.cacheSignature })`;
        }
        ddlSql += `comment on function ${func.getSignature()} is ${wrapText(dbComment)}`

        await this.query(ddlSql);
    }

    async dropFunction(func: DatabaseFunction) {
        const sql = `drop function if exists ${ func.getSignature() }`;
        await this.query(sql);
    }

    async createOrReplaceTrigger(trigger: DatabaseTrigger) {
        let ddlSql = `drop trigger if exists ${ trigger.getSignature() }`;
        
        ddlSql += ";";
        ddlSql += trigger.toSQL();

        ddlSql += ";";
        
        // TODO: move this code to Comparator
        let dbComment = "ddl-manager-sync";
        if ( trigger.comment ) {
            dbComment = trigger.comment + "\n" + "ddl-manager-sync";
        }
        if ( trigger.cacheSignature ) {
            dbComment += ` ddl-cache-signature(${ trigger.cacheSignature })`;
        }
        ddlSql += `comment on trigger ${trigger.getSignature()} is ${wrapText(dbComment)}`


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
            ) as ddl_manager_dmp on true

            limit 1
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
        let dbComment = "ddl-manager-sync";
        if ( column.comment ) {
            sql += `comment on column ${ column.getSignature() } is ${wrapText( column.comment )}`;
        }

        await this.query(sql);
    }

    async dropColumn(column: Column) {
        const sql = `
            alter table ${column.table} drop column if exists ${column.name};
        `;

        await this.query(sql);
    }

    async updateCachePackage(
        select: Select,
        forTable: TableReference,
        limit: number
    ) {
        const columnsToUpdate = select.columns.map(column =>
            column.name
        );

        const whereRowIsBroken = columnsToUpdate.map(columnName =>
            `${forTable.getIdentifier()}.${columnName} is distinct from ddl_manager_tmp.${columnName}`
        ).join(" or ");

        const selectBrokenRowsWithLimit = `
            select
                ${forTable.getIdentifier()}.id,
                ddl_manager_tmp.*
            from ${forTable}

            left join lateral (
                ${ select }
            ) as ddl_manager_tmp on true

            where
                ${ whereRowIsBroken }
            
            order by ${forTable.getIdentifier()}.id asc
            limit ${ limit }
        `;

        const sql = `
            update ${forTable} set
                (
                    ${ columnsToUpdate.join(", ") }
                ) = (
                    ${ select }
                )
            from (
                ${ selectBrokenRowsWithLimit }
            ) as ddl_manager_tmp

            where
                ddl_manager_tmp.id = ${forTable.getIdentifier()}.id
            
            returning ${forTable.getIdentifier()}.id
        `;
        const {rows} = await this.query(sql);

        return rows.length;
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

    end() {
        this.pgClient.end();
    }

    private async query(sql: string) {
        try {
            return await this.pgClient.query(sql);
        } catch(originalErr) {
            // redefine call stack
            const err = new Error(originalErr.message);
            (err as any).sql = sql;
            (err as any).originalError = originalErr;
            throw err;
        }
    }
}

function parseComment(row: {comment?: string}) {
    const comment = (row.comment || "").replace(/ddl-manager-sync$/i, "").trim();
    return comment || undefined;
}

function parseCacheSignature(row: {comment?: string}) {
    const comment = (row.comment || "").trim();
    const matchedResult = comment.match(/ddl-cache-signature\(([^\\)]+)\)/) || [];
    const cacheSignature = matchedResult[1];
    return cacheSignature;
}

function isFrozen(row: {comment?: string}) {
    const createByDDLManager = (
        row.comment &&
        /ddl-manager-sync/i.test(row.comment)
    );
    return !createByDDLManager;
}

function parseColumnNulls(columnRow: any) {
    if ( columnRow.is_nullable === "YES" ) {
        return true;
    } else {
        return false;
    }
}