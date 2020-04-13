import * as Path from "path";
import { Directory, IDirectoryParams } from "./Directory";
import { File } from "./File";
import { FSDriver } from "./FSDriver";
import { IDBOParser } from "../../common";

export interface IFSReaderParams {
    driver: FSDriver;
    parser: IDBOParser;
}

export class FSReader {
    private driver: FSDriver;
    private parser: IDBOParser;

    constructor(params: IFSReaderParams) {
        this.driver = params.driver;
        this.parser = params.parser;
    }

    async readDirectory(
        directoryPath: string
    ): Promise<Directory> {
        
        const dir = await this.driver.readDirectory(directoryPath);
        const files = await this.readDirectoryFiles(directoryPath, dir.files);
        const directories = await this.readSubDirectories(directoryPath, dir.directories);

        const directory = new Directory({
            name: getNameFromPath(directoryPath),
            path: directoryPath,
            files,
            directories
        });
        return directory;
    }

    private async readDirectoryFiles(
        directoryPath: string, 
        filesNames: string[]
    ): Promise<File[]> {
        const files = [];

        for (const fileName of filesNames) {
            if ( !isSqlFile(fileName) ) {
                continue;
            }

            const filePath = joinPath(directoryPath, fileName);
            const file = await this.readFile(filePath);
            
            files.push(file);
        }

        return files;
    }

    private async readSubDirectories(
        directoryPath: string, 
        directoriesNames: string[]
    ): Promise<Directory[]> {
        const directories = [];

        for (const directoryName of directoriesNames) {
            const subDirectoryPath = joinPath(directoryPath, directoryName);
            const subDirectory = await this.readDirectory(subDirectoryPath);

            directories.push( subDirectory );
        }

        return directories;
    }
    
    async readFile(filePath: string): Promise<File> {
        const sql = await this.driver.readFile(filePath);
        const dbObjects = this.parser.parse(sql);

        const fileName = filePath.split("/").pop();
        const file = new File({
            name: fileName,
            path: filePath,
            objects: dbObjects,
            sql
        });

        return file;
    }
}

function joinPath(...paths: string[]) {
    let joinedPath = Path.join(...paths);
    joinedPath = joinedPath.replace(/\\/g, "/");
    return joinedPath;
}

// "/path/to/some"   =>   "some"
function getNameFromPath(path: string) {
    const elems = path.split(/[/\\]/g);
    const lastElem = elems.pop();
    return lastElem;
}

function isSqlFile(filePath: string) {
    return /\.sql$/.test(filePath);
}