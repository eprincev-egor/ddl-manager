import fs from "fs";
import { Client } from "pg";
import {
    IState
} from "../interface";
import { IDatabaseDriver, ITableColumn } from "./interface";
import { FileParser } from "../parser";
import { PGTypes } from "./PGTypes";
import {
    DatabaseFunction,
    DatabaseTrigger,
    Table,
    Select,
    TableReference
} from "../ast";
import { getCheckFrozenFunctionSql } from "./postgres/getCheckFrozenFunctionSql";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import { getCheckFrozenTriggerSql } from "./postgres/getCheckFrozenTriggerSql";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { Table as TableSchema } from "./schema/Table";
import { flatMap } from "lodash";

const selectAllFunctionsSQL = fs.readFileSync(__dirname + "/postgres/select-all-functions.sql")
    .toString();
const selectAllTriggersSQL = fs.readFileSync(__dirname + "/postgres/select-all-triggers.sql")
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

        const {rows} = await this.pgClient.query(selectAllObjectsSQL);
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

        const {rows: columnsRows} = await this.pgClient.query(`
            select
                pg_columns.table_schema,
                pg_columns.table_name,
                pg_columns.table_schema || '.' || pg_columns.table_name as table_identify,
                pg_columns.column_name,
                pg_columns.column_default,
                pg_type.oid as column_type_oid,
                pg_columns.is_nullable
            from information_schema.columns as pg_columns

            left join pg_type on
                pg_type.typname = pg_columns.udt_name

            where
                pg_columns.table_schema != 'pg_catalog' and
                pg_columns.table_schema != 'information_schema'

            order by pg_columns.ordinal_position
        `);
        
        const database = new Database();
        for (const columnRow of columnsRows) {
            
            const tableId = {
                schema: columnRow.table_schema,
                name: columnRow.table_name
            };
            const table = database.getTable(tableId) || new TableSchema(
                columnRow.table_schema,
                columnRow.table_name
            );
            database.setTable(table);
            
            const columnType = this.types.getTypeById(
                columnRow.column_type_oid
            ) as string;
            const column = new Column(
                columnRow.column_name,
                columnType
            );
            // default: columnRow.column_default,
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

        try {
            await this.pgClient.query(ddlSql);
        } catch(err) {
            // redefine callstack
            const newErr = new Error(err.message);
            (newErr as any).originalError = err;
            
            throw newErr;
        }
    }

    async createOrReplaceFunction(func: DatabaseFunction) {
        try {
            // if func has other arguments,
            // then need drop function before replace
            // 
            // but can exists triggers or views who dependent on this function
            await this.forceDropFunction(func);
        } catch(err) {
            // 
        }

        let ddlSql = func.toSQL();
        
        ddlSql += ";";
        ddlSql += getUnfreezeFunctionSql(func);

        await this.pgClient.query(ddlSql);
    }

    async dropFunction(func: DatabaseFunction) {
        let ddlSql = "";

        // check frozen object
        const checkFrozenSql = getCheckFrozenFunctionSql( 
            func,
            `cannot drop frozen function ${ func.getSignature() }`
        );
        
        ddlSql = checkFrozenSql;

        ddlSql += ";";
        ddlSql += `drop function if exists ${ func.getSignature() }`;
        
        await this.pgClient.query(ddlSql);
    }

    async forceDropFunction(func: DatabaseFunction) {
        const sql = `drop function if exists ${ func.getSignature() }`;
        await this.pgClient.query(sql);
    }

    async createOrReplaceTrigger(trigger: DatabaseTrigger) {
        let ddlSql = `drop trigger if exists ${ trigger.getSignature() }`;
        
        ddlSql += ";";
        ddlSql += trigger.toSQL();

        ddlSql += ";";
        ddlSql += getUnfreezeTriggerSql(trigger);

        await this.pgClient.query(ddlSql);
    }

    async dropTrigger(trigger: DatabaseTrigger) {
        let ddlSql = "";
        
        // check frozen object
        const checkFrozenSql = getCheckFrozenTriggerSql( 
            trigger,
            `cannot drop frozen trigger ${ trigger.getSignature() }`
        );
        ddlSql = checkFrozenSql;

        ddlSql += ";";
        ddlSql += `drop trigger if exists ${ trigger.getSignature() }`;

        await this.pgClient.query(ddlSql);
    }

    async forceDropTrigger(trigger: DatabaseTrigger) {
        const sql = `drop trigger if exists ${ trigger.getSignature() }`;
        await this.pgClient.query(sql);
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
        const {fields} = await this.pgClient.query(sql);

        const columnsTypes: {[columnName: string]: string} = {};
        for (const field of fields) {
            const typeId = field.dataTypeID;
            const type = this.types.getTypeById(typeId) as string;

            columnsTypes[ field.name ] = type;
        }
        return columnsTypes;
    }

    async createOrReplaceColumn(table: Table, column: ITableColumn) {
        const sql = `
            alter table ${table} add column if not exists ${column.name} ${column.type} default ${ column.default };
        `;

        await this.pgClient.query(sql);
    }

    async dropColumn(table: Table, columnName: string) {
        const sql = `
            alter table ${table} drop column if exists ${columnName};
        `;

        await this.pgClient.query(sql);
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
        const {rows} = await this.pgClient.query(sql);

        return rows.length;
    }

    async createOrReplaceCacheTrigger(
        trigger: DatabaseTrigger,
        func: DatabaseFunction
    ) {
        const sql = `
            drop trigger if exists ${ trigger.getSignature() };
            drop function if exists ${ func.getSignature() };

            ${ func.toSQL() };
            ${ trigger.toSQL() };

            comment on function ${ func.getSignature() }
            is 'ddl-manager-sync ddl-cache-signature(${func.cacheSignature})';

            comment on trigger ${ trigger.getSignature() }
            is 'ddl-manager-sync: ddl-cache-signature(${trigger.cacheSignature})';
        `;

        await this.pgClient.query(sql);
    }

    async createOrReplaceHelperFunc(func: DatabaseFunction) {
        const sql = `
            drop function if exists ${ func.getSignature() };

            ${ func.toSQL() };

            comment on function ${ func.getSignature() }
            is 'ddl-manager-helper';
        `;

        await this.pgClient.query(sql);
    }

    end() {
        this.pgClient.end();
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