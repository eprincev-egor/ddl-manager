import { DatabaseFunction, DatabaseTrigger, Cache } from "./ast";
import { IFileContent } from "./fs/File";

// tslint:disable: no-console
export class Diff {
    readonly toDrop: IFileContent;
    readonly toCreate: IFileContent;

    static empty() {
        return new Diff({
            drop: {
                functions: [],
                triggers: [],
                cache: []
            },
            create: {
                functions: [],
                triggers: [],
                cache: []
            }
        });
    }

    private constructor(params: {drop: IFileContent, create: IFileContent}) {
        this.toDrop = params.drop;
        this.toCreate = params.create;
    }

    drop(state: Partial<IFileContent>) {
        if ( state.functions ) {
            state.functions.forEach(func => 
                this.dropFunction(func)
            );
        }

        if ( state.triggers ) {
            state.triggers.forEach(trigger => 
                this.dropTrigger(trigger)
            );
        }

        return this;
    }

    create(state: Partial<IFileContent>) {
        if ( state.functions ) {
            state.functions.forEach(func => 
                this.createFunction(func)
            );
        }

        if ( state.triggers ) {
            state.triggers.forEach(trigger => 
                this.createTrigger(trigger)
            );
        }

        return this;
    }

    dropFunction(func: DatabaseFunction) {
        this.toDrop.functions.push(func);
    }

    createFunction(func: DatabaseFunction) {
        this.toCreate.functions.push(func);
    }

    dropTrigger(trigger: DatabaseTrigger) {
        this.toDrop.triggers.push(trigger);
    }

    createTrigger(trigger: DatabaseTrigger) {
        this.toCreate.triggers.push(trigger);
    }

    dropCache(cache: Cache) {
        this.toDrop.cache.push(cache);
    }

    log() {
        this.toDrop.triggers.forEach((trigger) => {
            console.log("drop trigger " + trigger.getSignature());
        });
    
        this.toDrop.functions.forEach((func) => {
            console.log("drop function " + func.getSignature());
        });
        

        this.toCreate.functions.forEach((func) => {
            console.log("create function " + func.getSignature());
        });
    
        this.toCreate.triggers.forEach((trigger) => {
            console.log("create trigger " + trigger.getSignature());
        });
    }
    
}