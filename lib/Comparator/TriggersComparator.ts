import { uniq } from "lodash";
import { Migration } from "../Migrator/Migration";
import { IOutputTrigger } from "../cache/CacheTriggersBuilder";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";
import { FilesState } from "../fs/FilesState";
import { AbstractComparator } from "./AbstractComparator";
import { Table } from "../database/schema/Table";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";

export class TriggersComparator 
extends AbstractComparator {

    constructor(
        driver: IDatabaseDriver,
        database: Database,
        fs: FilesState,
        migration: Migration,
        private allCacheTriggers: IOutputTrigger[]
    ) {
        super(driver, database, fs, migration)
    }

    drop() {
        for (const table of this.database.tables) {
            for (const dbTrigger of table.triggers) {
                
                if ( dbTrigger.frozen ) {
                    continue;
                }
                if ( dbTrigger.cacheSignature ) {
                    continue;
                }

                const fsTriggers = this.fs.getTableTriggers(dbTrigger.table);
                const existsSameTriggerFromFile = fsTriggers.some(fileTrigger =>
                    fileTrigger.equal(dbTrigger)
                );
                if ( !existsSameTriggerFromFile ) {
                    this.migration.drop({
                        triggers: [dbTrigger]
                    });
                }
            }
        }
    }

    create() {
        for (const trigger of this.fs.allTriggers()) {
            const dbTable = this.database.getTable(trigger.table);
            const existsSameTriggerFromDb = dbTable?.triggers.some(dbTrigger =>
                dbTrigger.equal(trigger)
            );

            if ( !existsSameTriggerFromDb ) {
                this.migration.create({
                    triggers: [trigger]
                });
            }
        }

        this.fixBeforeUpdateTriggers();
    }

    private fixBeforeUpdateTriggers() {
        for (const dbTable of this.database.tables) {
            this.fixBeforeUpdateTriggersOn(dbTable);
        }
    }

    private fixBeforeUpdateTriggersOn(dbTable: Table) {
        const cacheTriggersAndFunctions = this.allCacheTriggers.filter(item =>
            item.trigger.table.equal(dbTable)
        );
        const cacheTriggers = cacheTriggersAndFunctions.map(item => item.trigger);
        const cacheProcedures = cacheTriggersAndFunctions.map(item => item.function);

        const dbTriggers = dbTable.triggers.filter(trigger => trigger.frozen);
        const fsTriggers = this.fs.getTableTriggers(dbTable);

        const allTriggers = [...cacheTriggers, ...fsTriggers, ...dbTriggers];

        const onUpdateTriggers = allTriggers.filter(trigger =>
            trigger.updateOf?.length
        );
        const beforeUpdateTriggers = onUpdateTriggers.filter(trigger => 
            trigger.before
        );

        const dependencyMap = this.buildDependencyMap(beforeUpdateTriggers, cacheProcedures);

        for (const trigger of onUpdateTriggers) {
            this.fixTrigger(dependencyMap, trigger);
        }
    }

    private buildDependencyMap(
        beforeUpdateTriggers: DatabaseTrigger[],
        cacheProcedures: DatabaseFunction[]
    ) {
        const dependencyMap: Record<string, string[]> = {};
        for (const trigger of beforeUpdateTriggers) {
            const procedure = (
                this.fs.getTriggerFunction(trigger) ||
                cacheProcedures.find(procedure => procedure.name === trigger.procedure.name) ||
                this.database.getFunctions(trigger.procedure.name)[0]
            )!;
            
            const updatedColumns = procedure.findAssignColumns();
            const dependenciesColumns = trigger.updateOf!;

            for (const updatedColumnName of updatedColumns) {
                dependencyMap[ updatedColumnName ] ??= [];
                dependencyMap[ updatedColumnName ].push( ...dependenciesColumns );
            }
        }

        return dependencyMap;
    }

    private fixTrigger(
        dependencyMap: Record<string, string[]>,
        trigger: DatabaseTrigger
    ) {
        const dbTable = this.database.getTable(trigger.table)!;

        const dependenciesColumns = findAllDependencies(dependencyMap, trigger.updateOf!);
        const fixedTrigger = trigger.clone({
            updateOf: uniq([
                ...trigger.updateOf!,
                ...dependenciesColumns
            ]).sort()
        });

        const existsSameTriggerFromDb = dbTable.triggers.some(dbTrigger =>
            dbTrigger.equal(trigger)
        );
        if ( !existsSameTriggerFromDb ) {
            this.migration.create({
                triggers: [fixedTrigger]
            });
        }
    }
}

function findAllDependencies(
    dependencyMap: Record<string, string[]>,
    columns: string[],
    scanned: Record<string, true> = {}
): string[] {
    const outputDependenciesColumns: string[] = [];

    for (const columnName of columns) {
        if ( columnName in scanned ) {
            continue;
        }
        scanned[ columnName ] = true;

        const dependenciesColumns = dependencyMap[ columnName ] ?? [];
        const subDependencies = findAllDependencies(dependencyMap, dependenciesColumns, scanned);

        outputDependenciesColumns.push( ...dependenciesColumns );
        outputDependenciesColumns.push( ...subDependencies );
    }

    return outputDependenciesColumns;
}