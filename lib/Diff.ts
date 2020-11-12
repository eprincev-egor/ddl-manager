import { DatabaseFunction, DatabaseTrigger, Cache } from "./ast";
import { IState } from "./interface";

export class Diff {
    readonly drop: IState;
    readonly create: IState;

    static empty() {
        return new Diff({
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });
    }

    constructor(params: {drop: IState, create: IState}) {
        this.drop = params.drop;
        this.create = params.create;
    }

    dropState(state: Partial<IState>) {
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
    }

    createState(state: Partial<IState>) {
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
    }

    dropFunction(func: DatabaseFunction) {
        this.drop.functions.push(func);
    }

    createFunction(func: DatabaseFunction) {
        this.create.functions.push(func);
    }

    dropTrigger(trigger: DatabaseTrigger) {
        this.drop.triggers.push(trigger);
    }

    createTrigger(trigger: DatabaseTrigger) {
        this.create.triggers.push(trigger);
    }

    createCache(cache: Cache) {
        if ( !this.create.cache ) {
            this.create.cache = [];
        }
        this.create.cache.push(cache);
    }

    log() {
        this.drop.triggers.forEach((trigger: any) => {
            const triggerIdentifySql = trigger.getSignature();
            // tslint:disable-next-line: no-console
            console.log("drop trigger " + triggerIdentifySql);
        });
    
        this.drop.functions.forEach((func: any) => {
            const funcIdentifySql = func.getSignature();
            // tslint:disable-next-line: no-console
            console.log("drop function " + funcIdentifySql);
        });
        
        this.create.functions.forEach((func: any) => {
            const funcIdentifySql = func.getSignature();
            // tslint:disable-next-line: no-console
            console.log("create function " + funcIdentifySql);
        });
    
        this.create.triggers.forEach((trigger: any) => {
            const triggerIdentifySql = trigger.getSignature();
            // tslint:disable-next-line: no-console
            console.log("create trigger " + triggerIdentifySql);
        });
    }
    
}