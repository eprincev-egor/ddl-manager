import {Collection, Model, Types} from "model-layer";

// tslint:disable: max-classes-per-file

export class FileModel extends Model<FileModel> {
    structure() {
        return {
            name: Types.String
        };
    }
}

export class FilesCollection extends Collection<FileModel> {
    Model() {
        return FileModel;
    }
}

export class FoldersCollection extends Collection<FolderModel> {
    Model() {
        return FolderModel;
    }
}

export class FolderModel extends Model<FolderModel> {
    structure() {
        return {
            name: Types.String,
            files: FilesCollection,
            folders: FoldersCollection
        };
    }
}