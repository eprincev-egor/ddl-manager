import {Model, Types} from "model-layer";

export default class FileModel extends Model<FileModel> {
    structure() {
        return {
            name: Types.String,
            path: Types.String,
            content: Types.String
        };
    }
}