import {
    DatabaseFunction,
    DatabaseTrigger,
    Select,
    TableReference,
    Table,
    Cache
} from "../../../lib/ast";
import { IDatabaseDriver, ITableColumn } from "../../../lib/database/interface";
import { Database } from "../../../lib/database/schema/Database";
import { IState } from "../../../lib/interface";

export class FakeDatabase
implements IDatabaseDriver {

    readonly state: IState;
    readonly columns: {
        [tableAndColumn: string]: ITableColumn;
    };
    private columnsTypes: {[column: string]: string};
    private rowsCountByTable: {[table: string]: number};
    private updatedPackages: {[table: string]: {
        limit: number;
    }[]};
    private allCache: Cache[];
    private columnsDrops: {[tableColumn: string]: boolean};

    constructor(state?: IState) {
        this.state = state || {
            functions: [],
            triggers: [],
            cache: []
        };
        this.columns = {};
        this.columnsTypes = {};
        this.rowsCountByTable = {};
        this.updatedPackages = {};
        this.allCache = [];
        this.columnsDrops = {};
    }

    async loadState(): Promise<IState> {
        return this.state;
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

    async dropFunction(func: DatabaseFunction): Promise<void> {
        const existentFuncIndex = this.state.functions.findIndex(someFunc =>
            someFunc.getSignature() === func.getSignature()
        );
        if ( existentFuncIndex !== -1 ) {
            this.state.functions.splice(existentFuncIndex, 1);
        }
    }

    async forceDropFunction(func: DatabaseFunction): Promise<void> {
        await this.dropFunction(func);
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

    async forceDropTrigger(trigger: DatabaseTrigger): Promise<void> {
        await this.dropTrigger(trigger);
    }

    async getCacheColumnsTypes(select: Select, forTable: TableReference): Promise<{ [columnName: string]: string; }> {
        const outputTypes: {[columnName: string]: string} = {};
        
        for (const column of select.columns) {
            const type = this.columnsTypes[ column.name ];
            outputTypes[ column.name ] = type;
        }

        return outputTypes;
    }

    async createOrReplaceColumn(table: Table, column: ITableColumn): Promise<void> {
        this.columns[ table.toString() + "." + column.key ] = column;
    }

    async dropColumn(table: Table, columnName: string): Promise<void> {
        delete this.columns[ table.toString() + "." + columnName ];
        this.columnsDrops[ table.toString() + "." + columnName ] = true;
    }

    async updateCachePackage(select: Select, forTable: TableReference, limit: number): Promise<number> {
        const table = forTable.table.toString();
        
        const alreadyUpdatedPackages = (this.updatedPackages[table] || []).slice();
        const updateRowsCount = alreadyUpdatedPackages.reduce((total, updated) => 
            total + updated.limit,
            0
        );
        const totalTableRowsCount = this.rowsCountByTable[ table ] || 0;
        const remainder = totalTableRowsCount - updateRowsCount;

        alreadyUpdatedPackages.push({
            limit
        });
        this.updatedPackages[table] = alreadyUpdatedPackages;

        return Math.min(remainder, limit);
    }

    unfreezeAll(dbState: IState): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async createOrReplaceCacheTrigger(trigger: DatabaseTrigger, func: DatabaseFunction) {
        await this.createOrReplaceFunction(func);
        await this.createOrReplaceTrigger(trigger);
    }

    async createOrReplaceHelperFunc(func: DatabaseFunction) {
        
    }

    async saveCacheMeta(allCache: Cache[]) {
        this.allCache = allCache;
    }

    async loadTables() {
        return new Database([]);
    }

    end() {
        // 
    }

    // test methods
    setColumnsTypes(types: {[column: string]: string}) {
        this.columnsTypes = types;
    }

    setRowsCount(table: string, rowsCount: number) {
        this.rowsCountByTable[ table ] = rowsCount;
    }

    getUpdatedPackages(table: string) {
        return this.updatedPackages[table];
    }

    wasDroppedColumn(table: string, columnName: string): boolean {
        return !!this.columnsDrops[ table + "." + columnName ];
    }
}
