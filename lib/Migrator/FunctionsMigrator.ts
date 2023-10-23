import { AbstractMigrator } from "./AbstractMigrator";

export class FunctionsMigrator extends AbstractMigrator {

    async drop() {
        await this.dropFunctions();
    }

    async create() {
        await this.createFunctions();
    }

    async createLogFuncs() {
        for (const func of this.migration.toCreate.functions) {
            try {
                await this.postgres.createOrReplaceLogFunction(func);
            } catch(error) {
                this.onError(func, error);
            }
        }
    }

    private async dropFunctions() {

        for (const func of this.migration.toDrop.functions) {
            // 2BP01
            try {
                await this.postgres.dropFunction(func);
            } catch(error) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = error.code === "2BP01";
                if ( !isCascadeError ) {
                    this.onError(func, error);
                }
            }
        }
    }

    private async createFunctions() {

        for (const func of this.migration.toCreate.functions) {
            try {
                await this.postgres.createOrReplaceFunction(func);
            } catch(error) {
                console.log(error);
                this.onError(func, error);
            }
        }
    }
}