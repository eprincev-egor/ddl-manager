import {Model, Types} from "model-layer";
import FunctionsCollection from "../objects/FunctionsCollection";
import TriggersCollection from "../objects/TriggersCollection";
import ViewsCollection from "../objects/ViewsCollection";
import TablesCollection from "../objects/TablesCollection";
import MigrationModel from "../migration/MigrationModel";
import MigrationController from "../migration/MigrationController";
import {IMigrationControllerParams} from "../migration/IMigrationControllerParams";
import FunctionModel from "../objects/FunctionModel";
import TableModel from "../objects/TableModel";
import ViewModel from "../objects/ViewModel";
import TriggerModel from "../objects/TriggerModel";
import BaseDBObjectModel from "../objects/BaseDBObjectModel";

export type IMigrationOptions = Omit<IMigrationControllerParams, "db" | "fs">;

export type TDBObject = BaseDBObjectModel<any>;

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

    
    protected addObjects(dbObjects: TDBObject[]) {
        for (const dbo of dbObjects) {
            this.addObject(dbo);
        }
    }

    protected addObject(dbo: TDBObject) {
        if ( dbo instanceof FunctionModel ) {
            this.row.functions.push(dbo);
        }
        else if ( dbo instanceof TableModel ) {
            this.row.tables.push(dbo);
        }
        else if ( dbo instanceof ViewModel ) {
            this.row.views.push(dbo);
        }
        else if ( dbo instanceof TriggerModel ) {
            this.row.triggers.push(dbo);
        }
    }

    protected removeObjects(dbObjects: TDBObject[]) {
        for (const dbo of dbObjects) {
            this.removeObject(dbo);
        }
    }

    protected removeObject(dbo: TDBObject) {
        if ( dbo instanceof FunctionModel ) {
            this.row.functions.remove(dbo);
        }
        else if ( dbo instanceof TableModel ) {
            this.row.tables.remove(dbo);
        }
        else if ( dbo instanceof ViewModel ) {
            this.row.views.remove(dbo);
        }
        else if ( dbo instanceof TriggerModel ) {
            this.row.triggers.remove(dbo);
        }
    }

    protected findObjects(filter: ((dbo: TDBObject) => boolean)): TDBObject[] {
        const funcs = this.row.functions.filter(filter);
        const tables = this.row.tables.filter(filter);
        const triggers = this.row.triggers.filter(filter);
        const views = this.row.views.filter(filter);

        return [
            ...funcs,
            ...tables,
            ...triggers,
            ...views
        ];
    }
}