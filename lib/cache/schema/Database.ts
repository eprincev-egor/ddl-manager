import { Table } from "./Table";

export class Database {
    readonly tables: Table[];
    
    constructor(tables: Table[]) {
        this.tables = tables;
    }

    getTable(tableId: {schema: string, name: string}) {
        return this.tables.find(table => 
            table.name === tableId.name &&
            table.schema === tableId.schema
        );
    }
}