import {Collection, Model, Types} from "model-layer";

export class FileModel extends Model<FileModel> {
    structure() {
        return {
            name: Types.String
        };
    }
}

export class FilesCollection extends Collection<FilesCollection> {
    Model() {
        return FileModel;
    }
}
