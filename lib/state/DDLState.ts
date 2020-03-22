import {Model, Types} from "model-layer";
import FunctionsCollection from "../objects/FunctionsCollection";
import TriggersCollection from "../objects/TriggersCollection";
import ViewsCollection from "../objects/ViewsCollection";
import TablesCollection from "../objects/TablesCollection";
import ExtensionsCollection from "../objects/ExtensionsCollection";
import MigrationModel from "../migration/MigrationModel";
import MainMigrationController from "../migration/MainMigrationController";
import {IMigrationControllerParams} from "../migration/IMigrationControllerParams";
import FunctionModel from "../objects/FunctionModel";
import TableModel from "../objects/TableModel";
import ViewModel from "../objects/ViewModel";
import TriggerModel from "../objects/TriggerModel";
import ExtensionModel from "../objects/ExtensionModel";
import BaseDBObjectModel from "../objects/BaseDBObjectModel";

export type IMigrationOptions = Omit<IMigrationControllerParams, "db" | "fs">;

export type TDBObject = BaseDBObjectModel<any>;

export default class DDLState<Child extends DDLState = DDLState<any>> 
extends Model<Child> {
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
            }),
            extensions: Types.Collection({
                Collection: ExtensionsCollection,
                default: () => new ExtensionsCollection()
            })
        };
    }

    generateMigration(
        dbState: DDLState, 
        options: IMigrationOptions
    ): MigrationModel {
        const fsState = this;

        const migrationController = new MainMigrationController({
            ...options,
            db: dbState,
            fs: fsState
        });

        return migrationController.generateMigration();
    }

    findExtensionsForTable(tableIdentify: string): ExtensionModel[] {
        return this.row.extensions.findExtensionsForTable(tableIdentify);
    }
    
    addObjects(dbObjects: TDBObject[]) {
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
        else if ( dbo instanceof ExtensionModel ) {
            this.row.extensions.push(dbo);
        }
    }

    removeObjects(dbObjects: TDBObject[]) {
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
        else if ( dbo instanceof ExtensionModel ) {
            this.row.extensions.remove(dbo);
        }
    }
}