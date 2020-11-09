import fs from "fs";
import { Client } from "pg";
import {
    IDatabaseDriver,
    IState
} from "../interface";
import { FileParser } from "../parser/FileParser";
import { DatabaseFunction } from "../ast/DatabaseFunction";
import { DatabaseTrigger } from "../ast/DatabaseTrigger";
import { getCheckFrozenFunctionSql } from "./postgres/getCheckFrozenFunctionSql";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import { getCheckFrozenTriggerSql } from "./postgres/getCheckFrozenTriggerSql";
import {
    trigger2sql
} from "../utils";

const selectAllFunctionsSQL = fs.readFileSync(__dirname + "/postgres/select-all-functions.sql")
    .toString();
const selectAllTriggersSQL = fs.readFileSync(__dirname + "/postgres/select-all-triggers.sql")
    .toString();

export class PostgresDriver
implements IDatabaseDriver {

    private pgClient: Client;
    private fileParser: FileParser;

    constructor(pgClient: Client) {
        this.pgClient = pgClient;
        this.fileParser = new FileParser();
    }

    async loadState() {
        const state: IState = {
            functions: await this.loadObjects<DatabaseFunction>(
                selectAllFunctionsSQL
            ),
            triggers: await this.loadObjects<DatabaseTrigger>(
                selectAllTriggersSQL
            )
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
        ddlSql += trigger2sql( trigger );

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