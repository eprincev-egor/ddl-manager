import fs from "fs";
import { Client } from "pg";
import {
    IDatabaseDriver,
    DatabaseFunctionType,
    DatabaseTriggerType,
    IState
} from "../interface";
import { FunctionParser } from "../parser/FunctionParser";
import { TriggerParser } from "../parser/TriggerParser";
import { getUnfreezeFunctionSql } from "./postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./postgres/getUnfreezeTriggerSql";
import {
    function2identifyJson,
    findCommentByFunction,
    findCommentByTrigger
} from "../utils";

const selectAllFunctionsSQL = fs.readFileSync(__dirname + "/postgres/select-all-functions.sql")
    .toString();
const selectAllTriggersSQL = fs.readFileSync(__dirname + "/postgres/select-all-triggers.sql")
    .toString();

export class PostgresDriver
implements IDatabaseDriver {

    private pgClient: Client;
    private triggerParser: TriggerParser;
    private functionParser: FunctionParser;

    constructor(pgClient: Client) {
        this.pgClient = pgClient;
        this.triggerParser = new TriggerParser();
        this.functionParser = new FunctionParser();
    }

    async loadState() {
        const state: IState = {
            triggers: [],
            functions: [],
            comments: []
        };

        const functions = await this.loadObjects<DatabaseFunctionType>(
            selectAllFunctionsSQL,
            this.functionParser
        );
        for (const func of functions) {

            state.functions.push(func);

            if ( func.comment ) {
                state.comments.push({
                    function: function2identifyJson(func),
                    comment: func.comment
                });
            }
        }

        const triggers = await this.loadObjects<DatabaseTriggerType>(
            selectAllTriggersSQL,
            this.triggerParser
        );
        for (const trigger of triggers) {

            state.triggers.push(trigger);

            if ( trigger.comment ) {
                state.comments.push({
                    trigger: {
                        schema: trigger.table.schema,
                        table: trigger.table.name,
                        name: trigger.name
                    },
                    comment: trigger.comment
                });
            }
        }

        return state;
    }

    private async loadObjects<T>(
        selectAllObjectsSQL: string,
        parser: FunctionParser | TriggerParser
    ): Promise<T[]> {
        const objects: any[] = [];

        const {rows} = await this.pgClient.query(selectAllObjectsSQL);
        for (const row of rows) {

            const object = parser.parse(row.ddl);
            const json = object.toJSON() as any;
 
            json.freeze = isFrozen(row);
            json.comment = parseComment(row);
        
            objects.push(json);
        }

        return objects;
    }

    async unfreezeAll(dbState: IState) {
        let ddlSql = "";

        const dbComments = dbState.comments || [];

        dbState.functions.forEach(func => {
            const comment = func.comment || findCommentByFunction(dbComments, func);

            ddlSql += getUnfreezeFunctionSql( func, comment );
            ddlSql += ";";
        });

        dbState.triggers.forEach(trigger => {
            const comment = trigger.comment || findCommentByTrigger(dbComments, trigger);

            ddlSql += getUnfreezeTriggerSql( trigger, comment );
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