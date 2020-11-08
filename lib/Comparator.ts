import assert from "assert";
import _ from "lodash";
import { DatabaseFunctionType, DatabaseTriggerType } from "./database/interface";

interface IState {
    functions: DatabaseFunctionType[];
    triggers: DatabaseTriggerType[];
    comments: any[];
}

interface IDiff {
    functions: any[];
    triggers: any[];
    comments?: any[];
}

export class Comparator {

    compare(dbState: IState, filesState: IState) {
    
        assert.ok(_.isObject(filesState), "undefined filesState");
        assert.ok(_.isArray(filesState.functions), "undefined filesState.functions");
        assert.ok(_.isArray(filesState.triggers), "undefined filesState.triggers");

        // TODO: any => type
        const drop: IDiff = {
            functions: [],
            triggers: []
        };
        const create: IDiff = {
            functions: [],
            triggers: []
        };

        this.dropFunctions(
            drop,
            create,
            dbState,
            filesState
        );
        this.dropTriggers(
            drop,
            create,
            dbState,
            filesState
        );
        this.dropComments(
            drop,
            create,
            dbState,
            filesState
        );
        
        this.createFunctions(
            drop,
            create,
            dbState,
            filesState
        );
        this.createTriggers(
            drop,
            create,
            dbState,
            filesState
        );
        this.createComments(
            drop,
            create,
            dbState,
            filesState
        );

        return {
            drop,
            create
        };
    }

    private dropFunctions(
        drop: IDiff,
        create: IDiff,
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
                    drop.triggers.push( fileTrigger );
                    // and create again
                    create.triggers.push( fileTrigger );
                });
            }
            
            
            drop.functions.push(func);
        }
    }

    private dropTriggers(
        drop: IDiff,
        create: IDiff,
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

        drop.triggers.push( ...triggersToDrop );
    }

    private dropComments(
        drop: IDiff,
        create: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        for (const comment of dbState.comments || []) {
            const existsSameCommentFromFile = (filesState.comments || []).some(fileComment =>
                equalComment(fileComment, comment)
            );

            if ( existsSameCommentFromFile ) {
                continue;
            }

            if ( !drop.comments ) {
                drop.comments = [];
            }
            drop.comments.push( comment );
        }
    }

    private createFunctions(
        drop: IDiff,
        create: IDiff,
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

            create.functions.push(func);
        }
    }

    private createTriggers(
        drop: IDiff,
        create: IDiff,
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

            create.triggers.push( trigger );
        }
    }

    private createComments(
        drop: IDiff,
        create: IDiff,
        dbState: IState,
        filesState: IState
    ) {
        for (const comment of filesState.comments || []) {
            
            const existsSameCommentFromDb = (dbState.comments || []).some(dbComment =>
                equalComment(dbComment, comment)
            );

            if ( existsSameCommentFromDb ) {
                continue;
            }

            if ( !create.comments ) {
                create.comments = [];
            }
            create.comments.push( comment );
        }
    }
}


function equalFunction(func1: any, func2: any) {
    return deepEqual(func1, func2);
}

function equalTrigger(trigger1: any, trigger2: any) {
    return deepEqual(trigger1, trigger2);
}

function equalComment(comment1: any, comment2: any) {
    return deepEqual(comment1, comment2);
}

function deepEqual(obj1: any, obj2: any) {
    try {
        assert.deepStrictEqual(obj1, obj2);
        return true;
    } catch(err) {
        return false;
    }
}
