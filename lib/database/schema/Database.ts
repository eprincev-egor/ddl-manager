import { flatMap } from "lodash";
import { Table } from "./Table";
import { TableID } from "./TableID";
import { DatabaseTrigger } from "./DatabaseTrigger";
import { DatabaseFunction } from "./DatabaseFunction";
import { Migration } from "../../Migrator/Migration";
import { Index } from "./Index";

export class Database {
    readonly tables: Table[];
    functions: DatabaseFunction[];
    readonly aggregators: string[];
    private tablesMap: Record<string, Table>;
    private functionsMap: Record<string, DatabaseFunction[]>;
    
    constructor(tables: Table[] = []) {
        this.tablesMap = {};
        this.tables = [];
        for (let table of tables) {
            table = table.clone();
            this.tables.push(table);
            this.tablesMap[ table.toString() ] = table;
        }

        this.functions = [];
        this.functionsMap = {};
        this.aggregators = [
            "count",
            "max",
            "min",
            "sum",
            "avg",
            "array_agg",
            "string_agg",
            "bool_or",
            "bool_and",
            "every"
        ];
    }

    getColumn(tableId: TableID, column: string) {
        const dbTable = this.tables.find(table => 
            table.name === tableId.name &&
            table.schema === tableId.schema
        );
        return dbTable?.getColumn(column);
    }

    getTable(tableId: TableID) {
        return this.tablesMap[ tableId.toString() ];
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
        if ( !this.tablesMap[ table.toString() ] ) {
            table = table.clone();
            this.tables.push( table );
            this.tablesMap[ table.toString() ] = table;
        }
    }

    addFunctions(functions: DatabaseFunction[]) {
        for (const func of functions) {
            this.functions.push(func);
            this.functionsMap[ func.name ] ??= [];
            this.functionsMap[ func.name ].push(func);
        }
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

        this.functions = this.functions.filter(existentFunc =>
            !migration.toDrop.functions.some(dropFunc =>
                existentFunc.getSignature() === dropFunc.getSignature()
            )
        );
        for (const dropFunc of migration.toDrop.functions) {
            this.functionsMap[dropFunc.name] = this.functionsMap[dropFunc.name].filter(existentFunc =>
                existentFunc.getSignature() !== dropFunc.getSignature()
            );
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

    allCacheTriggers() {
        return flatMap(this.tables, table => table.triggers)
            .filter(trigger => !!trigger.cacheSignature);
    }

    getFunctions(name: string) {
        return this.functionsMap[ name ] ?? [];
    }

}