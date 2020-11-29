import { Select } from "../ast";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { TableID } from "./schema/TableID";
import { TableReference } from "./schema/TableReference";
import { DatabaseTrigger } from "./schema/DatabaseTrigger";
import { DatabaseFunction  } from "./schema/DatabaseFunction";

// TODO: apply I from SOLID
export interface IDatabaseDriver {
    load(): Promise<Database>;
    unfreezeAll(dbState: Database): Promise<void>;
    createOrReplaceFunction(func: DatabaseFunction): Promise<void>;
    dropFunction(func: DatabaseFunction): Promise<void>;
    forceDropFunction(func: DatabaseFunction): Promise<void>;
    createOrReplaceTrigger(trigger: DatabaseTrigger): Promise<void>;
    dropTrigger(trigger: DatabaseTrigger): Promise<void>;
    forceDropTrigger(trigger: DatabaseTrigger): Promise<void>;
    getCacheColumnsTypes(select: Select, forTable: TableReference): Promise<{
        [columnName: string]: string
    }>;
    createOrReplaceColumn(tableID: TableID, column: Column): Promise<void>;
    dropColumn(table: TableID, columnName: string): Promise<void>;
    updateCachePackage(select: Select, forTable: TableReference, limit: number): Promise<number>;
    createOrReplaceCacheTrigger(
        trigger: DatabaseTrigger,
        func: DatabaseFunction
    ): Promise<void>;
    createOrReplaceHelperFunc(func: DatabaseFunction): Promise<void>;
    end(): void;
}
