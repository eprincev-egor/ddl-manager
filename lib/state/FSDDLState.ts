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

            folder: Types.Model({
                Model: FolderModel,
                default: () => new FolderModel()
            })
        };
    }

    getFileByPath(filePath: string): FileModel {
        return this.row.folder.getFileByPath(filePath) as FileModel;
    }

    getFileByContent(fileContent: string): FileModel {
        return this.row.folder.getFileByContent(fileContent) as FileModel;
    }

}
