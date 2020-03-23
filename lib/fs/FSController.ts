import {FSDDLState} from "../state/FSDDLState";
import {FSDriver} from "./FSDriver";
import {Parser} from "../parser/Parser";
import {FileModel} from "./FileModel";
import * as Path from "path";

// @see fs/index.ts
import {FolderModel} from "../fs/FolderModel";
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

        const folderModel = await this.readFolder(folderPath);
        this.state.setFolder( folderModel );
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
        this.state.removeFileByPath( fileModel.get("path") );
        this.state.addFile( fileModel );
    }

    private onFSAdd(fileModel: FileModel) {
        this.state.addFile(fileModel);
    }

    private onFSRename(oldFileModel: FileModel, newFileModel: FileModel) {
        this.state.removeFileByPath( oldFileModel.get("path") );
        this.state.addFile( newFileModel );
    }

    private async onFSUnlink(filePath: string) {
        this.state.removeFileByPath( filePath );
    }

    async readFolder(
        folderPath: string
    ): Promise<FolderModel> {
        const fs = this.driver;
        const folder = await fs.readFolder(folderPath);
        const {files, directories} = folder;
        
        const filesModels: FileModel[] = [];
        const foldersModels: FolderModel[] = [];

        for (const fileName of files) {
            if ( !/\.sql/.test(fileName) ) {
                continue;
            }

            const filePath = joinPath(folderPath, fileName);
            const fileModel = await this.readFile(filePath);
            
            filesModels.push(fileModel);
        }

        for (const folderName of directories) {
            const subFolderPath = joinPath(folderPath, folderName);

            const subFolderModel = await this.readFolder(subFolderPath);

            foldersModels.push( subFolderModel );
        }

        const outputFolderModel = new FolderModel({
            files: filesModels,
            folders: foldersModels
        });
        outputFolderModel.setPath(folderPath);

        return outputFolderModel;
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
}

function joinPath(...paths: string[]) {
    let joinedPath = Path.join(...paths);
    joinedPath = joinedPath.replace(/\\/g, "/");
    return joinedPath;
}