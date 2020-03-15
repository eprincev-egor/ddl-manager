import State, {TDBObject} from "./State";
import * as Path from "path";

// @see fs/index.ts
import FolderModel from "./fs/FolderModel";
import "./fs/FoldersCollection";

import FSDriver from "./fs/FSDriver";
import {Types} from "model-layer";
import FileModel from "./fs/FileModel";
import Parser from "./parser/Parser";

export default class FSState extends State<FSState> {
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

    constructor(inputData: FSState["TInputData"]) {
        super(inputData);

        this.row.driver.on("change", (path: string) => {
            this.onFSChange(path);
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

    private async onFSChange(filePath: string) {
        this.removeFile(filePath);
        await this.addFile(filePath);
    }

    private async onFSUnlink(filePath: string) {
        this.removeFile(filePath);
    }

    private async addFile(filePath: string) {
        const sql = await this.row.driver.readFile(filePath);
        const dbObjects = this.row.parser.parseFile(filePath, sql) as TDBObject[];

        const fileName = filePath.split("/").pop();
        const fileModel = new FileModel({
            name: fileName,
            path: filePath,
            content: sql
        });

        this.row.folder.row.files.add(fileModel);

        this.addObjects(dbObjects);
    }

    private removeFile(filePath: string) {
        const folder = this.row.folder;
        if ( !folder ) {
            return;
        }

        const fileModel = folder.getFile(filePath) as FileModel;
        if ( fileModel ) {
            let dbObjects: TDBObject[];

            dbObjects = this.findObjects((dbo) =>
                dbo.get("filePath") === filePath
            );
            this.removeObjects( dbObjects );

            folder.removeFile(filePath);
        }
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