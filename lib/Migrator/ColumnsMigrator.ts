import { AbstractMigrator } from "./AbstractMigrator";

export class ColumnsMigrator extends AbstractMigrator {

    async drop() {
        for (const column of this.migration.toDrop.columns) {
            // 2BP01
            try {
                await this.postgres.dropColumn(column);
            } catch(err) {
                this.onError(column, err);
            }
        }
    }

    async create() {
        for (const column of this.migration.toCreate.columns) {
            try {
                await this.postgres.createOrReplaceColumn(column);
            } catch(err) {
                this.onError(column, err);
            }
        }
    }
}