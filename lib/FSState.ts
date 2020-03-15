import State from "./State";

// @see fs/index.ts
import FolderModel from "./fs/FolderModel";
import "./fs/FoldersCollection";

import FSDriver from "./fs/FSDriver";
import {Types} from "model-layer";
import FileModel from "./fs/FileModel";
import Parser from "./parser/Parser";
import FunctionModel from "./objects/FunctionModel";
import TableModel from "./objects/TableModel";
import ViewModel from "./objects/ViewModel";
import TriggerModel from "./objects/TriggerModel";

type TDBObject = (
    FunctionModel |
    TableModel |
    ViewModel |
    TriggerModel
);

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
            folder: FolderModel
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
        const folderModel = await this.readFolder(folderPath);
        
        this.set({
            folder: folderModel
        });

        const files = folderModel.filterChildrenByInstance(FileModel);

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
        const fileRow: FileModel["TInputData"] = {
            name: fileName,
            path: filePath,
            content: sql
        };
        const fileModel = new FileModel(fileRow);

        let folder = this.row.folder;
        if ( !folder ) {
            const folderPath = filePath.split("/").slice(0, -1).join("/");
            const folderRow: FolderModel["TInputData"] = {
                path: (
                    folderPath === "." ? 
                        "./" : 
                        folderPath
                ),
                name: folderPath
                    .split(/[\\\/]/g)
                    .filter(name =>
                        name !== "" &&
                        name !== "."
                    )
                    .pop() || "",
                files: [],
                folders: []
            };
            folder = new FolderModel(folderRow);
            this.set({ folder });
        }
        folder.row.files.add(fileModel);

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

            dbObjects = this.row.functions.filter(dbo =>
                dbo.get("filePath") === filePath
            );
            this.removeObjects( dbObjects );

            dbObjects = this.row.tables.filter(dbo =>
                dbo.get("filePath") === filePath
            );
            this.removeObjects( dbObjects );

            dbObjects = this.row.triggers.filter(dbo =>
                dbo.get("filePath") === filePath
            );
            this.removeObjects( dbObjects );

            dbObjects = this.row.views.filter(dbo =>
                dbo.get("filePath") === filePath
            );
            this.removeObjects( dbObjects );

            folder.removeFile(filePath);
        }
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
    }

    private async readFolder(folderPath: string): Promise<FolderModel> {
        folderPath = folderPath.replace(/\/$/, "");

        const folderRow: FolderModel["TInputData"] = {
            path: (
                folderPath === "." ? 
                    "./" : 
                    folderPath
            ),
            name: folderPath
                .split(/[\\\/]/g)
                .filter(name =>
                    name !== "" &&
                    name !== "."
                )
                .pop() || "",
            files: [],
            folders: []
        };


        const fs = this.row.driver;
        const {files, directories} = await fs.readFolder(folderPath);

        for (const fileName of files) {
            if ( !/\.sql/.test(fileName) ) {
                continue;
            }

            const filePath = folderPath + "/" + fileName;
            const sql = await fs.readFile(filePath);
            
            const fileRow: FileModel["TInputData"] = {
                name: fileName,
                path: filePath,
                content: sql
            };

            folderRow.files.push(fileRow);
        }

        for (const folderName of directories) {
            const subFolderPath = folderPath + "/" + folderName;
            const subFolderModel = await this.readFolder( subFolderPath );

            folderRow.folders.push( subFolderModel );
        }

        return new FolderModel(folderRow);
    }

    prepareJSON(json) {
        delete json.driver;
        delete json.parser;
    }
}