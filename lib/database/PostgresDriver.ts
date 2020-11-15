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
    TableReference,
    Cache
} from "../ast";
import { getCheckFrozenFunctionSql } from "./postgres/getCheckFrozenFunctionSql";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import { getCheckFrozenTriggerSql } from "./postgres/getCheckFrozenTriggerSql";
import { wrapText } from "./postgres/wrapText";

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

    async loadState() {
        const state: IState = {
            functions: await this.loadObjects<DatabaseFunction>(
                selectAllFunctionsSQL
            ),
            triggers: await this.loadObjects<DatabaseTrigger>(
                selectAllTriggersSQL
            ),
            cache: await this.loadCache()
        };
        return state;
    }

    private async loadObjects<T>(selectAllObjectsSQL: string): Promise<T[]> {
        const objects: any[] = [];

        const {rows} = await this.pgClient.query(selectAllObjectsSQL);
        for (const row of rows) {

            const fileContent = this.fileParser.parse(row.ddl) as any;
            const json = fileContent.functions[0] || fileContent.triggers[0];
 
            json.frozen = isFrozen(row);
            json.comment = parseComment(row);
        
            objects.push(json);
        }

        return objects;
    }

    async unfreezeAll(dbState: IState) {
        let ddlSql = "";

        dbState.functions.forEach(func => {
            ddlSql += getUnfreezeFunctionSql( func );
            ddlSql += ";";
        });

        dbState.triggers.forEach(trigger => {
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
        let ddlSql = "";

        // check frozen object
        const checkFrozenSql = getCheckFrozenFunctionSql( 
            func,
            "",
            "drop"
        );
        
        ddlSql += checkFrozenSql;

        ddlSql += ";";
        ddlSql += func.toSQL();
        
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

    async createOrReplaceTrigger(trigger: DatabaseTrigger) {
        let ddlSql = "";
        
        // check frozen object
        const checkFrozenSql = getCheckFrozenTriggerSql( 
            trigger,
            `cannot replace frozen trigger ${ trigger.getSignature() }`
        );
        ddlSql = checkFrozenSql;


        ddlSql += ";";
        ddlSql += `drop trigger if exists ${ trigger.getSignature() }`;
        
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
            alter table ${table} drop column if exists ${column.key};
            alter table ${table} add column ${column.key} ${column.type} default ${ column.default };
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

    async createOrReplaceCacheTrigger(trigger: DatabaseTrigger, func: DatabaseFunction) {
        const sql = `
            drop trigger if exists ${ trigger.getSignature() };
            drop function if exists ${ func.getSignature() };

            ${ func.toSQL() };
            ${ trigger.toSQL() };

            comment on function ${ func.getSignature() }
            is 'ddl-manager-cache';

            comment on trigger ${ trigger.getSignature() }
            is 'ddl-manager-cache';
        `;

        await this.pgClient.query(sql);
    }

    async saveCacheMeta(allCache: Cache[]) {
        if ( !allCache.length ) {
            return;
        }

        const allCacheSQL = allCache
            .map(cache => 
                "(" + wrapText( cache.toString() ) + ")"
            );
        
        const sql = `
            drop table if exists ddl_manager_caches;
            create table ddl_manager_caches (
                cache_sql text
            );
            insert into ddl_manager_caches
                (cache_sql)
            values
                ${allCacheSQL.join(",\n")}
            ;
        `;
        await this.pgClient.query(sql);
    }

    private async loadCache() {
        try {
            const response = await this.pgClient.query(`select * from ddl_manager_caches`);
            const allCache = response.rows.map(row => {
                const cache = FileParser.parseCache(row.cache_sql);
                return cache;
            });
            return allCache;
        } catch(err) {
            return [];
        }
    }

    end() {
        this.pgClient.end();
    }
}

function parseComment(row: {comment?: string}) {
    const comment = (row.comment || "").replace(/ddl-manager-sync$/i, "").trim();
    return comment || undefined;
}

function isFrozen(row: {comment?: string}) {
    const createByDDLManager = (
        row.comment &&
        /ddl-manager-sync/i.test(row.comment)
    );
    return !createByDDLManager;
}