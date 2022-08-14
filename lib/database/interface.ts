import { Select } from "../ast";
import { Database } from "./schema/Database";
import { Column } from "./schema/Column";
import { TableReference } from "./schema/TableReference";
import { DatabaseTrigger } from "./schema/DatabaseTrigger";
import { DatabaseFunction  } from "./schema/DatabaseFunction";
import { Index } from "./schema/Index";
import { TableID } from "./schema/TableID";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";

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
    selectMinMax(table: TableID): Promise<MinMax>;
    /** update rows where id >= minId and id < maxId */
    updateCacheForRows(
        update: CacheUpdate,
        minId: number, maxId: number
    ): Promise<void>;
    updateCacheLimitedPackage(
        update: CacheUpdate,
        limit: number
    ): Promise<number>;
    createOrReplaceHelperFunc(func: DatabaseFunction): Promise<void>;
    dropIndex(index: Index): Promise<void>;
    createOrReplaceIndex(index: Index): Promise<void>;
    end(): void;
    disableTrigger(onTable: TableID, triggerName: string): Promise<void>;
    enableTrigger(onTable: TableID, triggerName: string): Promise<void>;
}

export interface MinMax {
    min: number | null;
    max: number | null;
}