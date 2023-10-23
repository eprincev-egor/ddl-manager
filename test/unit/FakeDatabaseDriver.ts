import { Expression, Select } from "../../lib/ast";
import { IDatabaseDriver, MinMax } from "../../lib/database/interface";
import { Database } from "../../lib/database/schema/Database";
import { Table } from "../../lib/database/schema/Table";
import { DatabaseFunction } from "../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../lib/database/schema/DatabaseTrigger";
import { Column } from "../../lib/database/schema/Column";
import { IFileContent } from "../../lib/fs/File";
import { Index } from "../../lib/database/schema/Index";
import { TableID } from "../../lib/database/schema/TableID";
import { CacheUpdate } from "../../lib/Comparator/graph/CacheUpdate";

export class FakeDatabaseDriver
implements IDatabaseDriver {

    readonly state: IFileContent;
    readonly columns: {
        [tableAndColumn: string]: {
            name: string;
            type: string;
            default: string | null;
        };
    };
    readonly indexes: {[table: string]: Index[]};

    private rowsCountByTable: {[table: string]: number};
    private updatedByLimit: {[table: string]: {
        limit: number;
    }[]};
    private tablesIds: {[table: string]: MinMax};
    private updatedByMinMax: {[table: string]: string[]};
    private columnsDrops: {[tableColumn: string]: boolean};

    constructor(state?: IFileContent) {
        this.state = state || {
            functions: [],
            triggers: [],
            cache: []
        };
        this.columns = {};
        this.rowsCountByTable = {};
        this.updatedByLimit = {};
        this.updatedByMinMax = {};
        this.tablesIds = {};
        this.columnsDrops = {};
        this.indexes = {};
    }

    async load() {
        const database = new Database([]);
        database.addFunctions(this.state.functions);

        for (const trigger of this.state.triggers) {
            const table = new Table(
                trigger.table.schema,
                trigger.table.name
            );
            database.setTable(table);
            database.addTrigger( trigger );
        }

        return database;
    }

    async query(sql: string) {
        return {rows: []}
    }

    async queryWithTimeout(sql: string, timeout: number) {
        return {rows: []}
    }

    async enableTrigger(onTable: TableID, triggerName: string): Promise<void> {
    }

    async disableTrigger(onTable: TableID, triggerName: string): Promise<void> {
    }

    async createOrReplaceFunction(func: DatabaseFunction): Promise<void> {
        const existentFuncIndex = this.state.functions.findIndex(someFunc =>
            someFunc.getSignature() === func.getSignature()
        );
        if ( existentFuncIndex !== -1 ) {
            this.state.functions[ existentFuncIndex ] = func;
        }
        else {
            this.state.functions.push( func );
        }
    }

    async createOrReplaceLogFunction(func: DatabaseFunction): Promise<void> {
        const existentFuncIndex = this.state.functions.findIndex(someFunc =>
            someFunc.getSignature() === func.getSignature()
        );
        if ( existentFuncIndex !== -1 ) {
            this.state.functions[ existentFuncIndex ] = func;
        }
        else {
            this.state.functions.push( func );
        }
    }

    async dropFunction(func: DatabaseFunction): Promise<void> {
        const existentFuncIndex = this.state.functions.findIndex(someFunc =>
            someFunc.getSignature() === func.getSignature()
        );
        if ( existentFuncIndex !== -1 ) {
            this.state.functions.splice(existentFuncIndex, 1);
        }
    }

    async createOrReplaceTrigger(trigger: DatabaseTrigger): Promise<void> {
        const existentTriggerIndex = this.state.triggers.findIndex(someTrigger =>
            someTrigger.getSignature() === trigger.getSignature()
        );
        if ( existentTriggerIndex !== -1 ) {
            this.state.triggers[ existentTriggerIndex ] = trigger;
        }
        else {
            this.state.triggers.push( trigger );
        }
    }

    async dropTrigger(trigger: DatabaseTrigger): Promise<void> {
        const existentTriggerIndex = this.state.triggers.findIndex(someFunc =>
            someFunc.getSignature() === trigger.getSignature()
        );
        if ( existentTriggerIndex !== -1 ) {
            this.state.triggers.splice(existentTriggerIndex, 1);
        }
    }

    async getType(expression: Expression): Promise<string> {
        return "";
    }

    async createOrReplaceColumn(column: Column): Promise<void> {
        this.columns[ column.getSignature() ] = {
            name: column.name,
            type: column.type.value,
            "default": column.default
        };
    }

    async dropColumn(column: Column): Promise<void> {
        delete this.columns[ column.getSignature() ];
        this.columnsDrops[ column.getSignature() ] = true;
    }

    async selectMinMax(tableId: TableID): Promise<MinMax> {
        const table = tableId.toString();
        const minMax = this.tablesIds[ table ] || {min: null, max: null};
        return minMax;
    }

    async selectNextIds(
        table: TableID,
        maxId: number,
        limit: number
    ): Promise<number[]> {
        const minMax = this.tablesIds[ table.toString() ] || {};

        const outputIds: number[] = [];

        while ( outputIds.length < limit ) {
            maxId--;
            if ( maxId < (minMax.min ?? 1) ) {
                break;
            }

            outputIds.push(maxId);
        }
        return outputIds.reverse();
    }
    
    async updateCacheForRows(
        update: CacheUpdate,
        minId: number,
        maxId: number
    ): Promise<void> {
        const table = update.table.table.toString();
        const updated = (this.updatedByMinMax[ table ] || []).slice();

        updated.push(`${minId} - ${maxId}`);
        this.updatedByMinMax[ table ] = updated;
    }

    async updateCacheLimitedPackage(
        update: CacheUpdate,
        limit: number
    ): Promise<number[]> {
        const table = update.table.table.toString();
        
        const alreadyUpdatedPackages = (this.updatedByLimit[table] || []).slice();
        const updateRowsCount = alreadyUpdatedPackages.reduce((total, updated) => 
            total + updated.limit,
            0
        );
        const totalTableRowsCount = this.rowsCountByTable[ table ] || 0;
        const remainder = totalTableRowsCount - updateRowsCount;

        alreadyUpdatedPackages.push({
            limit
        });
        this.updatedByLimit[table] = alreadyUpdatedPackages;

        const updatedCount = Math.min(remainder, limit);
        const outputIds: number[] = [];
        for (let i = 1; i < updatedCount; i++) {
            outputIds.push(i);
        }
        return outputIds;
    }

    unfreezeAll(dbState: Database): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async dropIndex(index: Index) {
        const table = index.table.toString();
        const tableIndexes = this.indexes[ table ] || [];
        const i = tableIndexes.findIndex(someIndex =>
            someIndex.getSignature() === index.getSignature()
        );
        if ( i !== -1 ) {
            tableIndexes.splice(i, 1);
        }
        this.indexes[ table ] = tableIndexes;
    }

    async createOrReplaceIndex(index: Index) {
        const table = index.table.toString();
        const tableIndexes = this.indexes[ table ] || [];
        tableIndexes.push(index);
        this.indexes[ table ] = tableIndexes;
    }

    end() {
        // 
    }

    async terminateActiveCacheUpdates() {
        // 
    }

    // test methods
    setRowsCount(table: string, rowsCount: number) {
        this.rowsCountByTable[ table ] = rowsCount;
    }

    getUpdatedPackages(table: string) {
        return this.updatedByLimit[table];
    }

    wasDroppedColumn(table: string, columnName: string): boolean {
        return !!this.columnsDrops[ table + "." + columnName ];
    }

    setTableMinMax(table: TableID, min: number, max: number): void {
        this.tablesIds[ table.toString() ] = {min, max};
    }

    getUpdates(table: TableID) {
        const updatedIds = (this.updatedByMinMax[ table.toString() ] || []).slice();
        return updatedIds;
    }
}
