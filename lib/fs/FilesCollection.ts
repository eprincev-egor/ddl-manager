import {Collection} from "model-layer";
import {FileModel} from "./FileModel";

export class FilesCollection extends Collection<FilesCollection> {
    Model() {
        return FileModel;
    }

    getFileByPath(fileName: string): FileModel {
        const existentFileModel = this.find((fileModel) =>
            fileModel.get("name") === fileName
        );

        return existentFileModel;
    }

    getFileByContent(fileContent: string): FileModel {
        const existentFileModel = this.find((fileModel) =>
            fileModel.get("content") === fileContent
        );

        return existentFileModel;
    }

    removeFile(fileName: string): void {
        const existentFileModel = this.getFileByPath(fileName);

        if ( existentFileModel ) {
            this.remove(existentFileModel);
        }
    }
}
