import { Column } from "./Column";
import { TableID } from "./TableID";
import { DatabaseTrigger } from "./DatabaseTrigger";

export class Table extends TableID {
    readonly columns: Column[];
    readonly triggers: DatabaseTrigger[];

    constructor(
        schema: string,
        name: string,
        columns: Column[] = [],
        triggers: DatabaseTrigger[] = []
    ) {
        super(schema, name);
        this.columns = columns;
        this.triggers = triggers;
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

    removeTrigger(dropTrigger: DatabaseTrigger) {
        const triggerIndex = this.triggers.findIndex(existentTrigger => 
            existentTrigger.equal(dropTrigger)
        );
        
        if ( triggerIndex !== -1 ) {
            this.triggers.splice(triggerIndex, 1);
        }
    }

    removeColumn(dropColumn: Column) {
        const columnIndex = this.columns.findIndex(existentColumn => 
            existentColumn.name === dropColumn.name
        );
        
        if ( columnIndex !== -1 ) {
            this.columns.splice(columnIndex, 1);
        }
    }

    clone() {
        const clone = new Table(
            this.schema,
            this.name,
            this.columns.map(column =>
                column.clone()
            )
        );
        return clone;
    }
}