import { DatabaseFunction, DatabaseTrigger, Select, TableReference, Table } from "../../../lib/ast";
import { IDatabaseDriver, ITableColumn } from "../../../lib/database//interface";
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
}