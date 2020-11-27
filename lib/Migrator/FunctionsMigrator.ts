import { FileParser } from "../parser";
import { AbstractMigrator } from "./AbstractMigrator";
import * as helperFunctions from "../database/postgres/helper-functions";

export class FunctionsMigrator extends AbstractMigrator {

    async drop() {
        await this.dropFunctions();
    }

    async create() {
        await this.createCacheHelpersFunctions();
        await this.createFunctions();
    }

    private async createCacheHelpersFunctions() {

        for (const helperFunctionSQL of Object.values(helperFunctions)) {
            const parsedFunction = FileParser.parseFunction(helperFunctionSQL);
            await this.postgres.createOrReplaceHelperFunc(parsedFunction);
        }
    }

    private async dropFunctions() {

        for (const func of this.diff.drop.functions) {
            // 2BP01
            try {
                await this.postgres.dropFunction(func);
            } catch(err) {
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                // cannot drop function my_func() because other objects depend on it
                const isCascadeError = err.code === "2BP01";
                if ( !isCascadeError ) {
                    this.onError(func, err);
                }
            }
        }
    }

    private async createFunctions() {

        for (const func of this.diff.create.functions) {
            try {
                await this.postgres.createOrReplaceFunction(func);
            } catch(err) {
                this.onError(func, err);
            }
        }
    }
}