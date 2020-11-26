import { Diff } from "../Diff";
import { IDatabaseDriver } from "../database/interface";

export class TriggersMigrator {
    private postgres: IDatabaseDriver;
    private outputErrors: Error[];
    private diff: Diff;

    constructor(
        postgres: IDatabaseDriver,
        diff: Diff,
        outputErrors: Error[]
    ) {
        this.postgres = postgres;
        this.diff = diff;
        this.outputErrors = outputErrors;
    }

    async drop() {
        await this.dropTriggers();
    }

    async create() {
        await this.createTriggers();
    }

    private async dropTriggers() {

        for (const trigger of this.diff.drop.triggers) {
            try {
                await this.postgres.dropTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
            }
        }
    }

    private async createTriggers() {

        for (const trigger of this.diff.create.triggers) {
            try {
                await this.postgres.createOrReplaceTrigger(trigger);
            } catch(err) {
                this.onError(trigger, err);
            }
        }
    }
    private onError(
        obj: {getSignature(): string},
        err: Error
    ) {
        // redefine callstack
        const newErr = new Error(obj.getSignature() + "\n" + err.message);
        (newErr as any).originalError = err;
        
        this.outputErrors.push(newErr);
    }
}