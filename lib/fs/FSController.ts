import FSDDLState from "../state/FSDDLState";
import FSDriver from "./FSDriver";
import Parser from "../parser/Parser";
import FileModel from "./FileModel";
import * as Path from "path";

// @see fs/index.ts
import FolderModel from "../fs/FolderModel";
import "../fs/FoldersCollection";

interface IFSControllerParams {
    state: FSDDLState;
    driver: FSDriver;
    parser: Parser;
}

export class FSController {
    private state: FSDDLState;
    private driver: FSDriver;
    private parser: Parser;

    constructor(params: IFSControllerParams) {
        this.state = params.state;
        this.driver = params.driver;
        this.parser = params.parser;

        this.listenDriverEvents();
    }

    listenDriverEvents() {
        this.driver.on("change", (path: string) => {
            this.onFSChangeOrRenameOrAdd(path);
        });

        this.driver.on("unlink", (path: string) => {
            this.onFSUnlink(path);
        });
    }

    async load(folderPath: string): Promise<void> {

        this.state.row.folder.setPath(folderPath);
        await this.readFolder(folderPath);
        
        const files = this.state.row.folder.filterChildrenByInstance(FileModel);

        for (const fileModel of files) {
            const dbObjects = fileModel.get("objects");

            this.state.addObjects(dbObjects);
        }
    }
    
    private async onFSChangeOrRenameOrAdd(filePath: string) {
        const fileModel = await this.readFile(filePath);
        const sql = fileModel.get("content");

        const existentFile = this.state.getFileByPath(filePath);
        const fileWithSameContent = this.state.getFileByContent(sql);

        let isRename = false;
        if ( fileWithSameContent ) {
            const oldFilePath = fileWithSameContent.get("path");
            const existsOldFile = await this.driver.existsFile(oldFilePath);

            if ( !existsOldFile ) {
                isRename = true;
            }
        }

        const isChange = (
            !isRename &&
            !!existentFile
        );

        if ( isRename ) {
            this.onFSRename(fileWithSameContent, fileModel);
        }
        else if ( isChange ) {
            this.onFSChange(fileModel);
        }
        else {
            this.onFSAdd(fileModel);
        }
    }

    private onFSChange(fileModel) {
        const filePath = fileModel.get("path");
        this.removeFile( filePath );

        this.addFile(fileModel);
    }

    private onFSAdd(fileModel: FileModel) {
        this.addFile(fileModel);
    }

    private onFSRename(oldFileModel: FileModel, newFileModel: FileModel) {
        this.removeFile(oldFileModel.get("path"));
        this.addFile(newFileModel);
    }

    private async onFSUnlink(filePath: string) {
        this.removeFile(filePath);
    }

    async readFile(filePath: string): Promise<FileModel> {
        const sql = await this.driver.readFile(filePath);
        const dbObjects = this.parser.parseFile(
            filePath,
            sql
        );

        const fileName = filePath.split("/").pop();
        const fileModel = new FileModel({
            name: fileName,
            path: filePath,
            content: sql,
            objects: dbObjects
        });

        return fileModel;
    }

    async readFolder(
        folderPath: string, 
        folderModel: FolderModel = this.state.row.folder
    ): Promise<void> {
        const fs = this.driver;
        const folder = await fs.readFolder(folderPath);
        const {files, directories} = folder;

        for (const fileName of files) {
            if ( !/\.sql/.test(fileName) ) {
                continue;
            }

            const filePath = joinPath(folderPath, fileName);
            const fileModel = await this.readFile(filePath);
            
            folderModel.row.files.push(fileModel);
        }

        for (const folderName of directories) {
            const subFolderPath = joinPath(folderPath, folderName);

            const subFolderModel = new FolderModel();
            subFolderModel.setPath(subFolderPath);

            await this.readFolder( subFolderPath, subFolderModel );

            folderModel.row.folders.push( subFolderModel );
        }

    }
    
    async addFile(fileModel: FileModel) {
        this.state.row.folder.row.files.add(fileModel);

        const dbObjects = fileModel.get("objects");
        this.state.addObjects(dbObjects);
    }

    removeFile(filePath: string) {
        const fileModel = this.state.getFileByPath(filePath);
        if ( !fileModel ) {
            return;
        }
    
        const dbObjects = fileModel.get("objects");
        this.state.removeObjects( dbObjects );

        this.state.row.folder.removeFile(filePath);
    }

}

function joinPath(...paths: string[]) {
    let joinedPath = Path.join(...paths);
    joinedPath = joinedPath.replace(/\\/g, "/");
    return joinedPath;
}