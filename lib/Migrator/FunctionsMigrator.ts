import { Diff } from "../Diff";
import { DatabaseFunction } from "../ast";
import { IDatabaseDriver } from "../database/interface";

export class FunctionsMigrator {
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
        await this.dropFunctions();
    }

    async create() {
        await this.createCacheHelpersFunctions();
        await this.createFunctions();
    }

    private async createCacheHelpersFunctions() {
        // TODO: parse from files
        
        const CM_ARRAY_REMOVE_ONE_ELEMENT = new DatabaseFunction({
            schema: "public",
            name: "cm_array_remove_one_element",
            args: [
                {name: "input_arr", type: "anyarray"},
                {name: "element_to_remove", type: "anyelement"}
            ],
            returns: {
                type: "anyarray"
            },
            body: `
declare element_position integer;
begin

    element_position = array_position(
        input_arr,
        element_to_remove
    );

    return (
        input_arr[:(element_position - 1)] || 
        input_arr[(element_position + 1):]
    );
    
end
            `
        });
        const CM_ARRAY_TO_STRING_DISTINCT = new DatabaseFunction({
            schema: "public",
            name: "cm_array_to_string_distinct",
            args: [
                {name: "input_arr", type: "text[]"},
                {name: "separator", type: "text"}
            ],
            returns: {
                type: "text"
            },
            body: `
begin
    
    return (
        select 
            string_agg(distinct input_value, separator)
        from unnest( input_arr ) as input_value
    );
end
            `
        });
        const CM_DISTINCT_ARRAY = new DatabaseFunction({
            schema: "public",
            name: "cm_distinct_array",
            args: [
                {name: "input_arr", type: "anyarray"}
            ],
            returns: {
                type: "anyarray"
            },
            body: `
begin
    return (
        select 
            array_agg(distinct input_value)
        from unnest( input_arr ) as input_value
    );
    
end
            `
        });
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