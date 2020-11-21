import { Column } from "./Column";

export class Table {
    readonly schema: string;
    readonly name: string;
    readonly columns: Column[];

    constructor(schema: string, name: string, columns: Column[]) {
        this.schema = schema;
        this.name = name;
        this.columns = columns;
    }

    getColumn(name: string) {
        return this.columns.find(column => column.name === name);
    }
}