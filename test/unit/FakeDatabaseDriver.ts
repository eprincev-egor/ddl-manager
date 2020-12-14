import { Select } from "../../lib/ast";
import { IDatabaseDriver } from "../../lib/database/interface";
import { Database } from "../../lib/database/schema/Database";
import { Table } from "../../lib/database/schema/Table";
import { DatabaseFunction } from "../../lib/database/schema/DatabaseFunction";
import { TableReference } from "../../lib/database/schema/TableReference";
import { DatabaseTrigger } from "../../lib/database/schema/DatabaseTrigger";
import { Column } from "../../lib/database/schema/Column";
import { IFileContent } from "../../lib/fs/File";

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
    private columnsTypes: {[column: string]: string};
    private rowsCountByTable: {[table: string]: number};
    private updatedPackages: {[table: string]: {
        limit: number;
    }[]};
    private columnsDrops: {[tableColumn: string]: boolean};

    constructor(state?: IFileContent) {
        this.state = state || {
            functions: [],
            triggers: [],
            cache: []
        };
        this.columns = {};
        this.columnsTypes = {};
        this.rowsCountByTable = {};
        this.updatedPackages = {};
        this.columnsDrops = {};
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

    unfreezeAll(dbState: Database): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async createOrReplaceHelperFunc(func: DatabaseFunction) {
        
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