import DDLState from "./DDLState";

// @see fs/index.ts
import FolderModel from "../fs/FolderModel";
import "../fs/FoldersCollection";

import {Types} from "model-layer";
import FileModel from "../fs/FileModel";

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
