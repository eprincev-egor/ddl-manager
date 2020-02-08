import {Collection, Model, Types} from "model-layer";
import {FilesCollection} from "./Files";

export class FolderModel extends Model<FolderModel> {
    structure() {
        return {
            name: Types.String,
            files: FilesCollection,
            folders: FoldersCollection
        };
    }
}

export class FoldersCollection extends Collection<FoldersCollection> {
    Model() {
        return FolderModel;
    }
}
