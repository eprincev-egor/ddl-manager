import { Table, ITableID } from "./Table";

export class Database {
    readonly tables: Table[];
    
    constructor(tables: Table[] = []) {
        this.tables = tables;
    }

    getTable(tableId: ITableID) {
        return this.tables.find(table => 
            table.name === tableId.name &&
            table.schema === tableId.schema
        );
    }

    setTable(table: Table) {
        const hasTable = this.getTable(table);
        if ( !hasTable ) {
            this.tables.push(table);
        }
    }
}