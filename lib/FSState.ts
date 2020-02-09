import State from "./State";

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
                constructor: Parser,
                toJSON: () => null
            }),
            folder: FolderModel
        };
    }

    async load(folderPath: string): Promise<void> {
        const folderModel = await this.readFolder(folderPath);
        
        this.set({
            folder: folderModel
        });
    }

    async readFolder(folderPath: string): Promise<FolderModel> {
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
        const {files, folders} = await fs.readFolder(folderPath);

        for (const fileName of files) {
            const filePath = folderPath + "/" + fileName;
            const fileContent = await fs.readFile(filePath);
            
            const fileRow: FileModel["TInputData"] = {
                name: fileName,
                path: filePath,
                content: fileContent
            };

            folderRow.files.push(fileRow);
        }

        for (const folderName of folders) {
            const subFolderPath = folderPath + "/" + folderName;
            const subFolderModel = await this.readFolder( subFolderPath );

            folderRow.folders.push( subFolderModel );
        }

        return new FolderModel(folderRow);
    }
}