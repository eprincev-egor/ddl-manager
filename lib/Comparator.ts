import assert from "assert";
import { isObject, isArray } from "lodash";
import { Diff } from "./Diff";
import { IState } from "./interface";

export class Comparator {

    static compare(dbState: IState, filesState: IState) {
        assert.ok(isObject(filesState), "undefined filesState");
        assert.ok(isArray(filesState.functions), "undefined filesState.functions");
        assert.ok(isArray(filesState.triggers), "undefined filesState.triggers");

        const comparator = new Comparator(dbState, filesState);
        return comparator.compare();
    }

    private diff: Diff;
    private dbState: IState;
    private filesState: IState;

    private constructor(dbState: IState, filesState: IState) {
        this.diff = Diff.empty();
        this.dbState = dbState;
        this.filesState = filesState;
    }

    compare() {
        this.dropFunctions();
        this.dropTriggers();
        
        this.createFunctions();
        this.createTriggers();
        this.createCache();

        return this.diff;
    }

    private dropFunctions() {
        for (const func of this.dbState.functions) {
            
            // ddl-manager cannot drop frozen function
            if ( func.frozen ) {
                continue;
            }

            const existsSameFuncFromFile = this.filesState.functions.some(fileFunc =>
                equalFunction(fileFunc, func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( func.returns.type === "trigger" ) {
                const depsTriggers = this.dbState.triggers.filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === func.schema &&
                        dbTrigger.procedure.name === func.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    const existsSameTriggerFromFile = this.filesState.triggers.some(fileTrigger =>
                        equalTrigger(fileTrigger, dbTrigger)
                    );

                    // if trigger has change, then he will dropped
                    // in next cycle
                    if ( !existsSameTriggerFromFile ) {
                        return false;
                    }

                    // we have trigger and he without changes
                    return true;
                });

                // drop
                this.diff.dropState({triggers: depsTriggers});
                // and create again
                this.diff.createState({triggers: depsTriggers});
            }
            
            
            this.diff.dropFunction(func);
        }
    }

    private dropTriggers() {
        const triggersCreatedFromDDLManager = this.dbState.triggers.filter(trigger =>
            !trigger.frozen
        );
        const triggersToDrop = triggersCreatedFromDDLManager.filter(trigger => {

            const existsSameTriggerFromFile = this.filesState.triggers.some(fileTrigger =>
                equalTrigger(fileTrigger, trigger)
            );
            return !existsSameTriggerFromFile;
        });

        this.diff.dropState({triggers: triggersToDrop});
    }

    private createFunctions() {
        for (const func of this.filesState.functions) {

            const existsSameFuncFromDb = this.dbState.functions.find(dbFunc =>
                equalFunction(dbFunc, func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            this.diff.createFunction(func);
        }
    }

    private createTriggers() {
        for (const trigger of this.filesState.triggers) {
            
            const existsSameTriggerFromDb = this.dbState.triggers.some(dbTrigger =>
                equalTrigger(dbTrigger, trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            this.diff.createTrigger(trigger);
        }
    }

    private createCache() {
        if ( this.filesState.cache ) {
            this.filesState.cache.forEach(cache => 
                this.diff.createCache(cache)
            );
        }
    }
}


function equalFunction(func1: any, func2: any) {
    return deepEqual(func1, func2);
}

function equalTrigger(trigger1: any, trigger2: any) {
    return deepEqual(trigger1, trigger2);
}

function deepEqual(obj1: any, obj2: any) {
    try {
        assert.deepStrictEqual(obj1, obj2);
        return true;
    } catch(err) {
        return false;
    }
}
