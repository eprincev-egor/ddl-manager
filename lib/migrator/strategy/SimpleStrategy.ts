import { AbstractStrategy } from "./AbstractStrategy";
import { IChanges } from "../../utils/compare";
import { IDBO } from "../../common";
import { Migration } from "../Migration";

export class SimpleStrategy<TObject extends IDBO>
extends AbstractStrategy<TObject> {

    buildMigration(
        migration: Migration, 
        changes: IChanges<TObject>
    ) {
        changes.removed.forEach(removedObject => {
            this.onRemove(migration, removedObject);
        });

        changes.changed.forEach(({prev, next}) => {
            this.onRemove(migration, prev);
            this.onCreate(migration, next);
        });

        changes.created.forEach(createdObject => {
            this.onCreate(migration, createdObject);
        });
    }

    private onRemove(migration: Migration, removedObject: TObject) {
        migration.addCommand("drop", removedObject);
    }

    private onCreate(migration: Migration, createdObject: TObject) {
        migration.addCommand("create", createdObject);
    }
}