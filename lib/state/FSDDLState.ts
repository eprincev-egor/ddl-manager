import {DDLState} from "./DDLState";
import {ExtensionsCollection} from "../objects/ExtensionsCollection";

import {IBaseMigratorParams} from "../migration/migrators/base-layers/BaseMigrator";
import {FunctionModel} from "../objects/FunctionModel";
import {TableModel} from "../objects/TableModel";
import {ViewModel} from "../objects/ViewModel";
import {TriggerModel} from "../objects/TriggerModel";
import {ExtensionModel} from "../objects/ExtensionModel";
import {BaseDBObjectModel} from "../objects/base-layers/BaseDBObjectModel";
import { Changes, IChanges } from "./Changes";

// @see fs/index.ts
import {FolderModel} from "../fs/FolderModel";
import "../fs/FoldersCollection";

import {Types} from "model-layer";
import {FileModel} from "../fs/FileModel";

export type IMigrationOptions = Omit<IBaseMigratorParams, "db" | "fs">;

export type TDBObject = BaseDBObjectModel<any>;

export class FSDDLState extends DDLState<FSDDLState> {
    structure() {
        return {
            ...super.structure(),

            folder: Types.Model({
                Model: FolderModel,
                default: () => new FolderModel()
            }),
            extensions: Types.Collection({
                Collection: ExtensionsCollection,
                default: () => new ExtensionsCollection()
            })
        };
    }

    findExtensionsForTable(tableIdentify: string): ExtensionModel[] {
        return this.row.extensions.findExtensionsForTable(tableIdentify);
    }
    
    getFileByPath(filePath: string): FileModel {
        return this.row.folder.getFileByPath(filePath) as FileModel;
    }

    getFileByContent(fileContent: string): FileModel {
        return this.row.folder.getFileByContent(fileContent) as FileModel;
    }

    setFolder(folder: FolderModel) {
        this.set({
            folder
        });
        
        const files = folder.filterChildrenByInstance(FileModel);
        for (const fileModel of files) {
            const dbObjects = fileModel.get("objects");
            this.addObjects(dbObjects);
        }
    }

    addFile(fileModel: FileModel) {
        this.row.folder.addFile(fileModel);

        const dbObjects = fileModel.get("objects");
        this.addObjects(dbObjects);
    }

    private addObjects(dbObjects: TDBObject[]) {
        for (const dbo of dbObjects) {
            this.addObject(dbo);
        }
    }

    private addObject(dbo: TDBObject) {
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

    removeFileByPath(filePath: string) {
        const fileModel = this.getFileByPath(filePath);

        if ( fileModel ) {
            const dbObjects = fileModel.get("objects");
            this.removeObjects( dbObjects );
        }

        this.row.folder.removeFile(filePath);
    }

    private removeObjects(dbObjects: TDBObject[]) {
        for (const dbo of dbObjects) {
            this.removeObject(dbo);
        }
    }

    private removeObject(dbo: TDBObject) {
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

    compareFunctionsWithDB(db: DDLState): IChanges<FunctionModel> {
        return this.compareWithDB<FunctionModel>("functions", db);
    }

    compareViewsWithDB(db: DDLState): IChanges<ViewModel> {
        return this.compareWithDB<ViewModel>("views", db);
    }

    compareTriggersWithDB(db: DDLState): IChanges<TriggerModel> {
        return this.compareWithDB<TriggerModel>("triggers", db);
    }

    compareTablesWithDB(db: DDLState): IChanges<TableModel> {
        return this.compareWithDB<TableModel>("tables", db);
    }

    private compareWithDB<TModel extends BaseDBObjectModel<any>>(
        key: keyof this["row"],
        db: DDLState
    ): IChanges<TModel> {
        const fs = this;
        const fsModels = (fs.row as any)[key].models;
        const dbModels = (db.row as any)[key].models;
        const changes = new Changes<TModel>();
        changes.detect(fsModels, dbModels);
        return changes;
    }
}
