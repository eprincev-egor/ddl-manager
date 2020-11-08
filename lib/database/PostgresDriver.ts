import {
    IDatabaseDriver,
    DatabaseFunctionType,
    DatabaseTriggerType
} from "./interface";
import { Client } from "pg";
import { FunctionParser } from "../parser/FunctionParser";
import { TriggerParser } from "../parser/TriggerParser";
import fs from "fs";

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

    async loadFunctions(): Promise<DatabaseFunctionType[]> {
        const functions = this.loadObjects(selectAllFunctionsSQL, this.functionParser);
        return functions;
    }

    async loadTriggers(): Promise<DatabaseTriggerType[]> {
        const triggers = await this.loadObjects(selectAllTriggersSQL, this.triggerParser);
        return triggers;
    }

    async loadObjects(
        selectAllObjectsSQL: string,
        parser: FunctionParser | TriggerParser
    ) {
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