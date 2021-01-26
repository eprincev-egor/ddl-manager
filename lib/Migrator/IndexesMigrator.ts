import { AbstractMigrator } from "./AbstractMigrator";

export class IndexesMigrator extends AbstractMigrator {

    async drop() {
        for (const index of this.migration.toDrop.indexes) {
            try {
                await this.postgres.dropIndex(index);
            } catch(err) {
                this.onError(index, err);
            }
        }
    }

    async create() {
        for (const index of this.migration.toCreate.indexes) {
            try {
                await this.postgres.createOrReplaceIndex(index);
            } catch(err) {
                this.onError(index, err);
            }
        }
    }
}