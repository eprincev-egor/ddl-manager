import { Column } from "./Column";
import { TableID } from "./TableID";
import { DatabaseTrigger } from "./DatabaseTrigger";
import { Index } from "./Index";

export class Table extends TableID {
    readonly columns: Column[];
    readonly triggers: DatabaseTrigger[];
    readonly indexes: Index[];

    constructor(
        schema: string,
        name: string,
        columns: Column[] = [],
        triggers: DatabaseTrigger[] = [],
        indexes: Index[] = []
    ) {
        super(schema, name);
        this.columns = columns;
        this.triggers = triggers;
        this.indexes = indexes;
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

    addIndex(index: Index) {
        this.indexes.push(index);
    }

    getTrigger(triggerName: string) {
        return this.triggers.find(trigger => 
            trigger.name === triggerName
        );
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