import { DatabaseTrigger } from "../../ast";
import { Column } from "./Column";

export interface ITableID {
    schema: string;
    name: string;
}

export class Table {
    readonly schema: string;
    readonly name: string;
    readonly columns: Column[];
    readonly triggers: DatabaseTrigger[];

    constructor(schema: string, name: string, columns: Column[] = []) {
        this.schema = schema;
        this.name = name;
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