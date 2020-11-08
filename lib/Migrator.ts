import { Client } from "pg";
import assert from "assert";
import {
    trigger2identifySql,
    trigger2dropSql,
    function2identifySql,
    function2dropSql,
    trigger2sql,
    function2sql
} from "./utils";
import { IDiff } from "./interface";
import { getCheckFreezeFunctionSql } from "./database/postgres/getCheckFreezeFunctionSql";
import { getCheckFreezeTriggerSql } from "./database/postgres/getCheckFreezeTriggerSql";
import { getUnfreezeFunctionSql } from "./database/postgres/getUnfreezeFunctionSql";
import { getUnfreezeTriggerSql } from "./database/postgres/getUnfreezeTriggerSql";

export class Migrator {
    private pgClient: Client;

    constructor(pgClient: Client) {
        this.pgClient = pgClient;
    }

    async migrate(diff: IDiff) {
        assert.ok(diff);

        const outputErrors: Error[] = [];

        await this.dropTriggers(diff, outputErrors);
        await this.dropFunctions(diff, outputErrors);

        await this.createFunctions(diff, outputErrors);
        await this.createTriggers(diff, outputErrors);

        return outputErrors;
    }

    private async dropTriggers(diff: IDiff, outputErrors: Error[]) {

        for (const trigger of diff.drop.triggers) {
            const triggerIdentifySql = trigger2identifySql( trigger );
            let ddlSql = "";
            
            // check freeze object
            const checkFreezeSql = getCheckFreezeTriggerSql( 
                trigger,
                `cannot drop freeze trigger ${ triggerIdentifySql }`
            );
            ddlSql = checkFreezeSql;

            ddlSql += ";";
            ddlSql += trigger2dropSql(trigger);

            try {
                await this.pgClient.query(ddlSql);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(triggerIdentifySql + "\n" + err.message);
                (newErr as any).originalError = err;

                outputErrors.push(newErr);
            }
        }
    }

    private async dropFunctions(diff: IDiff, outputErrors: Error[]) {

        for (const func of diff.drop.functions) {
            const funcIdentifySql = function2identifySql( func );
            let ddlSql = "";

            // check freeze object
            const checkFreezeSql = getCheckFreezeFunctionSql( 
                func,
                `cannot drop freeze function ${ funcIdentifySql }`
            );
            
            ddlSql = checkFreezeSql;

            ddlSql += ";";
            ddlSql += function2dropSql(func);
            // 2BP01
            try {
                await this.pgClient.query(ddlSql);
            } catch(err) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = err.code === "2BP01";
                if ( !isCascadeError ) {
                    // redefine callstack
                    const newErr = new Error(funcIdentifySql + "\n" + err.message);
                    (newErr as any).originalError = err;

                    outputErrors.push(newErr);
                }
            }
        }
    }

    private async createFunctions(diff: IDiff, outputErrors: Error[]) {

        for (const func of diff.create.functions) {
            let ddlSql = "";
            const funcIdentifySql = function2identifySql( func );

            // check freeze object
            const checkFreezeSql = getCheckFreezeFunctionSql( 
                func,
                "",
                "drop"
            );
            
            ddlSql += checkFreezeSql;

            ddlSql += ";";
            ddlSql += function2sql( func );
            
            ddlSql += ";";
            ddlSql += getUnfreezeFunctionSql(func);

            try {
                await this.pgClient.query(ddlSql);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(funcIdentifySql + "\n" + err.message);
                (newErr as any).originalError = err;
                
                outputErrors.push(newErr);
            }
        }
    }

    private async createTriggers(diff: IDiff, outputErrors: Error[]) {

        for (const trigger of diff.create.triggers) {
            const triggerIdentifySql = trigger2identifySql( trigger );
            let ddlSql = "";
            
            // check freeze object
            const checkFreezeSql = getCheckFreezeTriggerSql( 
                trigger,
                `cannot replace freeze trigger ${ triggerIdentifySql }`
            );
            ddlSql = checkFreezeSql;


            ddlSql += ";";
            ddlSql += trigger2dropSql( trigger );
            
            ddlSql += ";";
            ddlSql += trigger2sql( trigger );

            ddlSql += ";";
            ddlSql += getUnfreezeTriggerSql(trigger);

            try {
                await this.pgClient.query(ddlSql);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(triggerIdentifySql + "\n" + err.message);
                (newErr as any).originalError = err;
                
                outputErrors.push(newErr);
            }
        }
    }
}