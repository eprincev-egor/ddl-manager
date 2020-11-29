import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";

interface IChanges {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
}

// tslint:disable: no-console
export class Migration {
    readonly toDrop: IChanges;
    readonly toCreate: IChanges;

    static empty() {
        return new Migration();
    }

    private constructor() {
        this.toDrop = {functions: [], triggers: []};
        this.toCreate = {functions: [], triggers: []};
    }

    drop(state: Partial<IChanges>) {
        if ( state.functions ) {
            state.functions.forEach(func => 
                this.toDrop.functions.push(func)
            );
        }

        if ( state.triggers ) {
            state.triggers.forEach(trigger => 
                this.toDrop.triggers.push(trigger)
            );
        }

        return this;
    }

    create(state: Partial<IChanges>) {
        if ( state.functions ) {
            state.functions.forEach(func => 
                this.toCreate.functions.push(func)
            );
        }

        if ( state.triggers ) {
            state.triggers.forEach(trigger => 
                this.toCreate.triggers.push(trigger)
            );
        }

        return this;
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