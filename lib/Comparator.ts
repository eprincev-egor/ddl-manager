import assert from "assert";
import _ from "lodash";
import { IState, IDiff } from "./interface";

export class Comparator {

    compare(dbState: IState, filesState: IState) {
    
        assert.ok(_.isObject(filesState), "undefined filesState");
        assert.ok(_.isArray(filesState.functions), "undefined filesState.functions");
        assert.ok(_.isArray(filesState.triggers), "undefined filesState.triggers");

        const diff: IDiff = {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        };

        this.dropFunctions(
            diff,
            dbState,
            filesState
        );
        this.dropTriggers(
            diff,
            dbState,
            filesState
        );
        
        this.createFunctions(
            diff,
            dbState,
            filesState
        );
        this.createTriggers(
            diff,
            dbState,
            filesState
        );

        return diff;
    }

    private dropFunctions(
        diff: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        for (const func of dbState.functions) {
            
            // ddl-manager cannot drop freeze function
            if ( func.freeze ) {
                continue;
            }

            const existsSameFuncFromFile = filesState.functions.some(fileFunc =>
                equalFunction(fileFunc, func)
            );

            if ( existsSameFuncFromFile ) {
                continue;
            }

            // for drop function, need drop trigger, who call it function
            if ( func.returns.type === "trigger" ) {
                const depsTriggers = dbState.triggers.filter(dbTrigger => {
                    const isDepsTrigger = (
                        dbTrigger.procedure.schema === func.schema &&
                        dbTrigger.procedure.name === func.name
                    );

                    if ( !isDepsTrigger ) {
                        return false;
                    }
                    
                    const existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
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

                depsTriggers.forEach(fileTrigger => {
                    // drop
                    diff.drop.triggers.push( fileTrigger );
                    // and create again
                    diff.create.triggers.push( fileTrigger );
                });
            }
            
            
            diff.drop.functions.push(func);
        }
    }

    private dropTriggers(
        diff: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        const triggersCreatedFromDDLManager = dbState.triggers.filter(trigger =>
            !trigger.freeze
        );
        const triggersToDrop = triggersCreatedFromDDLManager.filter(trigger => {

            const existsSameTriggerFromFile = filesState.triggers.some(fileTrigger =>
                equalTrigger(fileTrigger, trigger)
            );
            return !existsSameTriggerFromFile;
        });

        diff.drop.triggers.push( ...triggersToDrop );
    }

    private createFunctions(
        diff: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        for (const func of filesState.functions) {

            const existsSameFuncFromDb = dbState.functions.find(dbFunc =>
                equalFunction(dbFunc, func)
            );

            if ( existsSameFuncFromDb ) {
                continue;
            }

            diff.create.functions.push(func);
        }
    }

    private createTriggers(
        diff: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        for (const trigger of filesState.triggers) {
            
            const existsSameTriggerFromDb = dbState.triggers.some(dbTrigger =>
                equalTrigger(dbTrigger, trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            diff.create.triggers.push( trigger );
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
