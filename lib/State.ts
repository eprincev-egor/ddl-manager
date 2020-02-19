import {Model, Types} from "model-layer";
import FunctionsCollection from "./objects/FunctionsCollection";
import TriggersCollection from "./objects/TriggersCollection";
import ViewsCollection from "./objects/ViewsCollection";
import TablesCollection from "./objects/TablesCollection";
import MigrationModel from "./migration/MigrationModel";
import MigrationController, {IMigrationControllerParams} from "./migration/MigrationController";

export type IMigrationOptions = Omit<IMigrationControllerParams, "db" | "fs">;

export default class State<Child extends State = State<any>> extends Model<Child> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: () => new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: () => new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: () => new ViewsCollection()
            }),
            tables: Types.Collection({
                Collection: TablesCollection,
                default: () => new TablesCollection()
            })
        };
    }

    generateMigration(
        dbState: State, 
        options: IMigrationOptions
    ): MigrationModel {
        const fsState = this;

        const migrationController = new MigrationController({
            ...options,
            db: dbState,
            fs: fsState
        });

        return migrationController.generateMigration();
    }
}