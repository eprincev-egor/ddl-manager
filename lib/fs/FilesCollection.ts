import {Collection} from "model-layer";
import FileModel from "./FileModel";

export default class FilesCollection extends Collection<FilesCollection> {
    Model() {
        return FileModel;
    }
}
