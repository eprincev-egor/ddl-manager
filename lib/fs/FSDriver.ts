import fs from "fs";

async function readFile(path: string): Promise<string> {
    return new Promise((resolve, reject) => {
        fs.readFile(path, (err, data) => {
            if ( err ) {
                reject(err);
            }
            else {
                resolve(data.toString());
            }
        });
    });
}

async function readdir(path: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
        fs.readdir(path, (err, files) => {
            if ( err ) {
                reject(err);
            }
            else {
                resolve(files);
            }
        });
    });
}

async function isDirectory(path: string): Promise<boolean> {
    return new Promise((resolve, reject) => {
        fs.stat(path, (err, stat) => {
            resolve( stat.isDirectory() );
        });
    });
}

export interface IDirContent {
    files: string[]; 
    folders: string[];
}

export default class FSDriver {
    async readFile(filePath: string): Promise<string> {
        const fileContent = await readFile(filePath);
        return fileContent;
    }

    async readFolder(folderPath: string): Promise<IDirContent> {
        const filesOrFolders = await readdir(folderPath);
        const files: string[] = [];
        const folders: string[] = [];

        for (const fileOrFolder of filesOrFolders) {
            const path = folderPath + "/" + fileOrFolder;
            const isFolder = await isDirectory(path);

            if ( isFolder ) {
                folders.push( fileOrFolder );
            }
            else {
                files.push( fileOrFolder );
            }
        }

        return {
            files,
            folders
        };
    }
}