import {Model, Types} from "model-layer";
import FilesCollection from "./FilesCollection";
import FileModel from "./FileModel";
import index from "./index";

export default class FolderModel extends Model<FolderModel> {
    structure() {
        return {
            path: Types.String,
            name: Types.String,
            files: FilesCollection,
            folders: Types.Collection({
                Collection: index.FoldersCollection,
                default: () => new index.FoldersCollection()
            })
        };
    }

    removeFile(filePath: string): void {
        const pathParts = filePath.split("/");
        
        const fileName = pathParts.pop();
        this.row.files.removeFile( fileName );
    }

    getFile(filePath: string): FileModel {
        const pathParts = filePath.split("/");

        const fileName = pathParts.pop();
        const fileModel = this.row.files.getFile(fileName);
        return fileModel as FileModel;
    }
}

index.FolderModel = FolderModel;