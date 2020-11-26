import { Diff } from "../Diff";
import { IDatabaseDriver } from "../database/interface";

export abstract class AbstractMigrator {
    
    abstract drop(): Promise<void>;
    abstract create(): Promise<void>;

    protected postgres: IDatabaseDriver;
    protected outputErrors: Error[];
    protected diff: Diff;

    constructor(
        postgres: IDatabaseDriver,
        diff: Diff,
        outputErrors: Error[]
    ) {
        this.postgres = postgres;
        this.diff = diff;
        this.outputErrors = outputErrors;
    }

    protected onError(
        obj: {getSignature(): string},
        err: Error
    ) {
        // redefine callstack
        const newErr = new Error(obj.getSignature() + "\n" + err.message);
        (newErr as any).originalError = err;
        
        this.outputErrors.push(newErr);
    }
}