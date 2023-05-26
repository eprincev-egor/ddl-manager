import { AbstractComparator } from "./AbstractComparator";
import { flatMap } from "lodash";

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

                const existsSameTriggerFromFile = flatMap(this.fs.files, file => 
                    file.content.triggers
                ).some(fileTrigger =>
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

            const existsSameTriggerFromDb = dbTable && dbTable.triggers.some(dbTrigger =>
                dbTrigger.equal(trigger)
            );

            if ( existsSameTriggerFromDb ) {
                continue;
            }

            this.migration.create({
                triggers: [trigger]
            });
        }
    }
}