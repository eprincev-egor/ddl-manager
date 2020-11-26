import { AbstractMigrator } from "./AbstractMigrator";

export class TriggersMigrator extends AbstractMigrator {
    
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
}