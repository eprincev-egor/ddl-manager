import { Column } from "./Column";
import { TableID } from "./TableID";
import { DatabaseTrigger } from "./DatabaseTrigger";

export class Table extends TableID {
    readonly columns: Column[];
    readonly triggers: DatabaseTrigger[];

    constructor(schema: string, name: string, columns: Column[] = []) {
        super(schema, name);
        this.columns = columns;
        this.triggers = [];
    }

    getColumn(name: string) {
        return this.columns.find(column => column.name === name);
    }

    addColumn(column: Column) {
        this.columns.push(column);
    }

    addTrigger(trigger: DatabaseTrigger) {
        this.triggers.push(trigger);
    }
}