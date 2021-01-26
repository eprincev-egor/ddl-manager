import { flatMap } from "lodash";
import { Table } from "./Table";
import { TableID } from "./TableID";
import { DatabaseTrigger } from "./DatabaseTrigger";
import { DatabaseFunction } from "./DatabaseFunction";
import { Migration } from "../../Migrator/Migration";
import { Index } from "./Index";

export class Database {
    readonly tables: Table[];
    readonly functions: DatabaseFunction[];
    readonly aggregators: string[];
    
    constructor(tables: Table[] = []) {
        this.tables = tables.map(table => 
            table.clone()
        );
        this.functions = [];
        this.aggregators = [
            "count",
            "max",
            "min",
            "sum",
            "avg",
            "array_agg",
            "string_agg",
            "bool_or",
            "bool_and"
        ];
    }

    getTable(tableId: TableID) {
        return this.tables.find(table => 
            table.name === tableId.name &&
            table.schema === tableId.schema
        );
    }

    getTriggersByProcedure(procedure: {schema: string, name: string, args: string[]}) {
        return flatMap(this.tables, (table) => {
            return table.triggers.filter(trigger =>
                trigger.procedure.schema === procedure.schema &&
                trigger.procedure.name === procedure.name
            );
        });
    }

    setTable(table: Table) {
        const hasTable = this.getTable(table);
        if ( !hasTable ) {
            this.tables.push( table.clone() );
        }
    }

    addFunctions(functions: DatabaseFunction[]) {
        this.functions.push(
            ...functions
        );
    }

    addTrigger(trigger: DatabaseTrigger) {
        const table = this.getTable(trigger.table);
        if ( !table ) {
            throw new Error(`unknown table "${ trigger.table }" for trigger "${ trigger.name }"`)
        }

        table.addTrigger(trigger);
    }

    addIndex(index: Index) {
        const table = this.getTable(index.table);
        if ( !table ) {
            throw new Error(`unknown table "${ index.table }" for index "${ index.name }"`)
        }

        table.addIndex(index);
    }

    applyMigration(migration: Migration) {
        this.addFunctions(migration.toCreate.functions);

        for (const trigger of migration.toCreate.triggers) {
            this.addTrigger(trigger);
        }

        for (const column of migration.toCreate.columns) {
            const table = this.getTable(column.table);
            if ( table ) {
                table.addColumn(column);
            }
        }

        for (const dropFunc of migration.toDrop.functions) {
            const funcIndex = this.functions.findIndex(existentFunc => 
                existentFunc.equal(dropFunc)
            );

            if ( funcIndex !== -1 ) {
                this.functions.splice(funcIndex, 1);
            }
        }

        for (const trigger of migration.toDrop.triggers) {
            const table = this.getTable(trigger.table);
            if ( table ) {
                table.removeTrigger(trigger);
            }
        }

        for (const column of migration.toDrop.columns) {
            const table = this.getTable(column.table);
            if ( table ) {
                table.removeColumn(column);
            }
        }
    }

    addAggregators(newAggregators: string[]) {
        for (const aggName of newAggregators) {
            if ( !this.aggregators.includes(aggName) )     {
                this.aggregators.push(aggName);
            }
        }
    }
}