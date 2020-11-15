import {
    Cache,
    Select,
    Table,
    TableReference,
    DatabaseTrigger,
    DatabaseFunction 
} from "../ast";
import { IState } from "../interface";

export interface ITableColumn {
    key: string;
    type: string;
    default: string | null;
}

export interface IDatabaseDriver {
    loadState(): Promise<IState>;
    unfreezeAll(dbState: IState): Promise<void>;
    createOrReplaceFunction(func: DatabaseFunction): Promise<void>;
    dropFunction(func: DatabaseFunction): Promise<void>;
    createOrReplaceTrigger(trigger: DatabaseTrigger): Promise<void>;
    dropTrigger(trigger: DatabaseTrigger): Promise<void>;
    getCacheColumnsTypes(select: Select, forTable: TableReference): Promise<{
        [columnName: string]: string
    }>;
    createOrReplaceColumn(table: Table, column: ITableColumn): Promise<void>;
    updateCachePackage(select: Select, forTable: TableReference, limit: number): Promise<number>;
    createOrReplaceCacheTrigger(trigger: DatabaseTrigger, func: DatabaseFunction): Promise<void>;
    saveCacheMeta(allCache: Cache[]): Promise<void>;
    end(): void;
}
