import {Model, Types} from "model-layer";
import FilesCollection from "./FilesCollection";
import FileModel from "./FileModel";
import index from "./index";

export default class FolderModel extends Model<FolderModel> {
    structure() {
        return {
            path: Types.String,
            name: Types.String,
            files: Types.Collection({
                Collection: FilesCollection,
                default: () => new FilesCollection()
            }),
            folders: Types.Collection({
                Collection: index.FoldersCollection,
                default: () => new index.FoldersCollection()
            })
        };
    }

    setPath(dirPath: string) {
        dirPath = dirPath.replace(/\/$/, "");

        const dirName = dirPath
            .split(/[\\\/]/g)
            .filter(name =>
                name !== "" &&
                name !== "."
            )
            .pop() || "";
        
        const changes = {
            name: dirName,
            path: dirPath === "." ? 
                "./" : 
                dirPath
        };

        this.set(changes);
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