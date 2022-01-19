import { Select } from "../ast";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { TableReference } from "./schema/TableReference";
import { DatabaseTrigger } from "./schema/DatabaseTrigger";
import { DatabaseFunction  } from "./schema/DatabaseFunction";
import { Index } from "./schema/Index";
import { TableID } from "./schema/TableID";

// TODO: apply I from SOLID
export interface IDatabaseDriver {
    load(): Promise<Database>;
    unfreezeAll(dbState: Database): Promise<void>;
    createOrReplaceFunction(func: DatabaseFunction): Promise<void>;
    createOrReplaceLogFunction(func: DatabaseFunction): Promise<void>;
    dropFunction(func: DatabaseFunction): Promise<void>;
    createOrReplaceTrigger(trigger: DatabaseTrigger): Promise<void>;
    dropTrigger(trigger: DatabaseTrigger): Promise<void>;
    getCacheColumnsTypes(select: Select, forTable: TableReference): Promise<{
        [columnName: string]: string
    }>;
    createOrReplaceColumn(column: Column): Promise<void>;
    dropColumn(column: Column): Promise<void>;
    selectIds(table: TableID, offset: number, limit: number): Promise<number[]>;
    updateCacheForRows(select: Select, forTable: TableReference, ids: number[]): Promise<void>;
    updateCacheLimitedPackage(select: Select, forTable: TableReference, limit: number): Promise<number>;
    createOrReplaceHelperFunc(func: DatabaseFunction): Promise<void>;
    dropIndex(index: Index): Promise<void>;
    createOrReplaceIndex(index: Index): Promise<void>;
    end(): void;
}
