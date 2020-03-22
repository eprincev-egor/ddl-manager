import DDLState, {TDBObject} from "./DDLState";
import * as Path from "path";

// @see fs/index.ts
import FolderModel from "../fs/FolderModel";
import "../fs/FoldersCollection";

import FSDriver from "../fs/FSDriver";
import {Types} from "model-layer";
import FileModel from "../fs/FileModel";
import Parser from "../parser/Parser";

export default class FSDDLState extends DDLState<FSDDLState> {
    structure() {
        return {
            ...super.structure(),

            driver: Types.CustomClass({
                constructor: FSDriver,
                toJSON: () => null
            }),
            parser: Types.CustomClass({
                constructor: Parser as (new () => Parser),
                toJSON: () => null
            }),
            folder: Types.Model({
                Model: FolderModel,
                default: () => new FolderModel()
            })
        };
    }

    constructor(inputData: FSDDLState["TInputData"]) {
        super(inputData);

        this.row.driver.on("change", (path: string) => {
            this.onFSChangeOrRenameOrAdd(path);
        });

        this.row.driver.on("unlink", (path: string) => {
            this.onFSUnlink(path);
        });
    }

    async load(folderPath: string): Promise<void> {
        const parser = this.row.parser;

        this.row.folder.setPath(folderPath);
        await this.readFolder(folderPath);
        
        const files = this.row.folder.filterChildrenByInstance(FileModel);

        for (const fileModel of files) {
            const filePath = fileModel.get("path");
            const sql = fileModel.get("content");
            const dbObjects = parser.parseFile(filePath, sql) as TDBObject[];

            this.addObjects(dbObjects);
        }
    }

    private async onFSChangeOrRenameOrAdd(filePath: string) {
        const fileModel = await this.readFile(filePath);
        const sql = fileModel.get("content");

        const existentFile = this.getFileByPath(filePath);
        const fileWithSameContent = this.getFileByContent(sql);

        let isRename = false;
        if ( fileWithSameContent ) {
            const oldFilePath = fileWithSameContent.get("path");
            const existsOldFile = await this.row.driver.existsFile(oldFilePath);

            if ( !existsOldFile ) {
                isRename = true;
            }
        }

        const isChange = (
            !isRename &&
            !!existentFile
        );

        if ( isRename ) {
            this.onFSRename(fileWithSameContent, fileModel);
        }
        else if ( isChange ) {
            this.onFSChange(fileModel);
        }
        else {
            this.onFSAdd(fileModel);
        }
    }

    private onFSChange(fileModel) {
        const filePath = fileModel.get("path");
        this.removeFile( filePath );

        this.addFile(fileModel);
    }

    private onFSAdd(fileModel: FileModel) {
        this.addFile(fileModel);
    }

    private onFSRename(oldFileModel: FileModel, newFileModel: FileModel) {
        this.removeFile(oldFileModel.get("path"));
        this.addFile(newFileModel);
    }

    private async onFSUnlink(filePath: string) {
        this.removeFile(filePath);
    }

    private async addFile(fileModel: FileModel) {
        this.row.folder.row.files.add(fileModel);

        const dbObjects = this.parseFile(fileModel);
        this.addObjects(dbObjects);
    }

    private parseFile(fileModel: FileModel): TDBObject[] {
        const filePath = fileModel.get("path");
        const sql = fileModel.get("content");
        const dbObjects = this.row.parser.parseFile(filePath, sql) as TDBObject[];

        return dbObjects;
    }

    private async readFile(filePath: string): Promise<FileModel> {
        const sql = await this.row.driver.readFile(filePath);

        const fileName = filePath.split("/").pop();
        const fileModel = new FileModel({
            name: fileName,
            path: filePath,
            content: sql
        });

        return fileModel;
    }

    private removeFile(filePath: string) {
        const fileModel = this.getFileByPath(filePath);
        if ( !fileModel ) {
            return;
        }
    
        let dbObjects: TDBObject[];

        dbObjects = this.findObjects((dbo) =>
            dbo.get("filePath") === filePath
        );
        this.removeObjects( dbObjects );

        this.row.folder.removeFile(filePath);
    }

    private getFileByPath(filePath: string): FileModel {
        return this.row.folder.getFileByPath(filePath) as FileModel;
    }

    private getFileByContent(fileContent: string): FileModel {
        return this.row.folder.getFileByContent(fileContent) as FileModel;
    }

    private async readFolder(
        folderPath: string, 
        folderModel: FolderModel = this.row.folder
    ): Promise<void> {
        const fs = this.row.driver;
        const folder = await fs.readFolder(folderPath);
        const {files, directories} = folder;

        for (const fileName of files) {
            if ( !/\.sql/.test(fileName) ) {
                continue;
            }

            const filePath = joinPath(folderPath, fileName);
            const sql = await fs.readFile(filePath);
            
            const fileRow: FileModel["TInputData"] = {
                name: fileName,
                path: filePath,
                content: sql
            };

            folderModel.row.files.push(fileRow);
        }

        for (const folderName of directories) {
            const subFolderPath = joinPath(folderPath, folderName);

            const subFolderModel = new FolderModel();
            subFolderModel.setPath(subFolderPath);

            await this.readFolder( subFolderPath, subFolderModel );

            folderModel.row.folders.push( subFolderModel );
        }

    }

    prepareJSON(json) {
        delete json.driver;
        delete json.parser;
    }
}

function joinPath(...paths: string[]) {
    let joinedPath = Path.join(...paths);
    joinedPath = joinedPath.replace(/\\/g, "/");
    return joinedPath;
}