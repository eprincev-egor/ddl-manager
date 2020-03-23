import fs from "fs";
import { EventEmitter } from "events";

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

async function checkIsDirectory(path: string): Promise<boolean> {
    return new Promise((resolve, reject) => {
        fs.stat(path, (err, stat) => {
            resolve( stat.isDirectory() );
        });
    });
}

async function existsFile(path: string): Promise<boolean> {
    return new Promise((resolve) => {
        fs.exists(path, (exists) => {
            resolve( exists );
        });
    });
}

export interface IDirectory {
    files: string[]; 
    directories: string[];
}

export class FSDriver extends EventEmitter {
    async existsFile(filePath: string): Promise<boolean> {
        const exists = await existsFile(filePath);
        return exists;
    }

    async readFile(filePath: string): Promise<string> {
        const fileContent = await readFile(filePath);
        return fileContent;
    }

    async readFolder(folderPath: string): Promise<IDirectory> {
        const filesOrDirectories = await readdir(folderPath);
        const files: string[] = [];
        const directories: string[] = [];

        for (const fileOrDirName of filesOrDirectories) {
            const path = folderPath + "/" + fileOrDirName;
            const isDirectory = await checkIsDirectory(path);

            if ( isDirectory ) {
                const dirName = fileOrDirName;
                directories.push( dirName );
            }
            else {
                const fileName = fileOrDirName;
                files.push( fileName );
            }
        }

        return {
            files,
            directories
        };
    }
}