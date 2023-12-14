import { AbstractComparator } from "./AbstractComparator";

export class TriggersComparator 
extends AbstractComparator {

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
    }
}