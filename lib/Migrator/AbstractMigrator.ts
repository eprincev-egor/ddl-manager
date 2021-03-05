import { Migration } from "./Migration";
import { IDatabaseDriver } from "../database/interface";
import { Database as DatabaseStructure } from "../database/schema/Database";

export abstract class AbstractMigrator {
    
    abstract drop(): Promise<void>;
    abstract create(): Promise<void>;

    protected postgres: IDatabaseDriver;
    protected outputErrors: Error[];
    protected migration: Migration;
    protected database: DatabaseStructure;

    constructor(
        postgres: IDatabaseDriver,
        migration: Migration,
        database: DatabaseStructure,
        outputErrors: Error[]
    ) {
        this.postgres = postgres;
        this.migration = migration;
        this.database = database;
        this.outputErrors = outputErrors;
    }

    protected onError(
        obj: {getSignature(): string},
        err: Error
    ) {
        // redefine callstack
        const newErr = new Error(
            obj.getSignature() + "\n" + 
            err.message + "\n\n" +
            ((err as any).sql || "")
        );
        (newErr as any).originalError = err;
        
        this.outputErrors.push(newErr);
    }
}