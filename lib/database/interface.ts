import {
    Select,
    Table,
    TableReference,
    DatabaseTrigger,
    DatabaseFunction 
} from "../ast";
import { IState } from "../interface";
import { Database as DatabaseSchema } from "./schema/Database";

export interface ITableColumn {
    name: string;
    type: string;
    default: string | null;
}

// TODO: apply I from SOLID
export interface IDatabaseDriver {
    loadFunctions(): Promise<DatabaseFunction[]>;
    loadTriggers(): Promise<DatabaseTrigger[]>;
    loadTables(): Promise<DatabaseSchema>;
    unfreezeAll(dbState: IState): Promise<void>;
    createOrReplaceFunction(func: DatabaseFunction): Promise<void>;
    dropFunction(func: DatabaseFunction): Promise<void>;
    forceDropFunction(func: DatabaseFunction): Promise<void>;
    createOrReplaceTrigger(trigger: DatabaseTrigger): Promise<void>;
    dropTrigger(trigger: DatabaseTrigger): Promise<void>;
    forceDropTrigger(trigger: DatabaseTrigger): Promise<void>;
    getCacheColumnsTypes(select: Select, forTable: TableReference): Promise<{
        [columnName: string]: string
    }>;
    createOrReplaceColumn(table: Table, column: ITableColumn): Promise<void>;
    dropColumn(table: Table, columnName: string): Promise<void>;
    updateCachePackage(select: Select, forTable: TableReference, limit: number): Promise<number>;
    createOrReplaceCacheTrigger(
        cacheSignature: string,
        trigger: DatabaseTrigger,
        func: DatabaseFunction
    ): Promise<void>;
    createOrReplaceHelperFunc(func: DatabaseFunction): Promise<void>;
    end(): void;
}
