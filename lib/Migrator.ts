import { DbState } from "./DbState";
import {
    findCommentByFunction,
    findCommentByTrigger,
    findTriggerByComment,
    findFunctionByComment,

    comment2dropSql,
    trigger2identifySql,
    trigger2dropSql,
    function2identifySql,
    function2dropSql,
    trigger2sql,
    function2sql
} from "./utils";
import { Client } from "pg";
import { IDiff } from "./Comparator";
import assert from "assert";

export class Migrator {
    private pgClient: Client;

    constructor(pgClient: Client) {
        this.pgClient = pgClient;
    }

    async migrate(diff: IDiff) {
        assert.ok(diff);

        const outputErrors: Error[] = [];

        await this.dropComments(diff, outputErrors);
        await this.dropTriggers(diff, outputErrors);
        await this.dropFunctions(diff, outputErrors);

        await this.createFunctions(diff, outputErrors);
        await this.createTriggers(diff, outputErrors);

        return outputErrors;
    }

    private async dropComments(diff: IDiff, outputErrors: Error[]) {

        for (const comment of diff.drop.comments || []) {
            let ddlSql = "";

            const dropTrigger = findTriggerByComment(diff.drop.triggers, comment);
            if ( dropTrigger ) {
                continue;
            }
            const dropFunc = findFunctionByComment(diff.drop.functions, comment);
            if ( dropFunc ) {
                continue;
            }

            ddlSql += comment2dropSql(comment);

            try {
                await this.pgClient.query(ddlSql);
            } catch(err) {
                // redefine callstack
                const newErr = new Error(err.message);
                (newErr as any).originalError = err;
                
                outputErrors.push(newErr);
            }
        }

    }

    private async dropTriggers(diff: IDiff, outputErrors: Error[]) {

        for (const trigger of diff.drop.triggers) {
            const triggerIdentifySql = trigger2identifySql( trigger );
            let ddlSql = "";
            
            // check freeze object
            const checkFreezeSql = DbState.getCheckFreezeTriggerSql( 
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
            const checkFreezeSql = DbState.getCheckFreezeFunctionSql( 
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
            const checkFreezeSql = DbState.getCheckFreezeFunctionSql( 
                func,
                "",
                "drop"
            );
            
            ddlSql += checkFreezeSql;

            ddlSql += ";";
            ddlSql += function2sql( func );
            
            ddlSql += ";";
            // comment on function ddl-manager-sync
            const comment = findCommentByFunction(
                diff.create.comments || [],
                func
            );

            ddlSql += DbState.getUnfreezeFunctionSql(func, comment);

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
            const checkFreezeSql = DbState.getCheckFreezeTriggerSql( 
                trigger,
                `cannot replace freeze trigger ${ triggerIdentifySql }`
            );
            ddlSql = checkFreezeSql;


            ddlSql += ";";
            ddlSql += trigger2dropSql( trigger );
            
            ddlSql += ";";
            ddlSql += trigger2sql( trigger );

            ddlSql += ";";
            // comment on trigger ddl-manager-sync
            const comment = findCommentByTrigger(
                diff.create.comments || [],
                trigger
            );
            ddlSql += DbState.getUnfreezeTriggerSql(trigger, comment);

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