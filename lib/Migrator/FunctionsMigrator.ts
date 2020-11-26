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

        const CM_ARRAY_REMOVE_ONE_ELEMENT = FileParser.parseFunction(
            helperFunctions.CM_ARRAY_REMOVE_ONE_ELEMENT
        );

        const CM_ARRAY_TO_STRING_DISTINCT = FileParser.parseFunction(
            helperFunctions.CM_ARRAY_TO_STRING_DISTINCT
        );
        
        const CM_DISTINCT_ARRAY = FileParser.parseFunction(
            helperFunctions.CM_DISTINCT_ARRAY
        );

        // TODO: cm_is_distinct_arrays
        // TODO: cm_get_deleted_elements
        // TODO: cm_get_inserted_elements

        await this.postgres.createOrReplaceHelperFunc(CM_ARRAY_REMOVE_ONE_ELEMENT);
        await this.postgres.createOrReplaceHelperFunc(CM_ARRAY_TO_STRING_DISTINCT);
        await this.postgres.createOrReplaceHelperFunc(CM_DISTINCT_ARRAY);
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