import {Model, Types} from "model-layer";
import FilesCollection from "./FilesCollection";
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
}

index.FolderModel = FolderModel;