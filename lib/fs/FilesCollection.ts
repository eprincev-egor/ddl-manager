import {Collection} from "model-layer";
import FileModel from "./FileModel";

export default class FilesCollection extends Collection<FilesCollection> {
    Model() {
        return FileModel;
    }

    getFile(fileName: string): FileModel {
        const existentFileModel = this.find((fileModel) =>
            fileModel.get("name") === fileName
        );

        return existentFileModel;
    }

    removeFile(fileName: string): void {
        const existentFileModel = this.getFile(fileName);

        if ( existentFileModel ) {
            this.remove(existentFileModel);
        }
    }
}
