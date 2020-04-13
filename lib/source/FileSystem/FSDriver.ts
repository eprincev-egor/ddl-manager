import fs from "fs";
import { EventEmitter } from "events";

export interface IDirectory {
    files: string[]; 
    directories: string[];
}

export class FSDriver extends EventEmitter {
    async existsFile(filePath: string): Promise<boolean> {
        return new Promise((resolve) => {
            fs.exists(filePath, (exists) => {
                resolve( exists );
            });
        });
    }

    async readFile(filePath: string): Promise<string> {
        return new Promise((resolve, reject) => {
            fs.readFile(filePath, (err, data) => {
                if ( err ) {
                    reject(err);
                }
                else {
                    resolve(data.toString());
                }
            });
        });
    }

    async readDirectory(directoryPath: string): Promise<IDirectory> {
        const filesOrDirectories = await readdir(directoryPath);
        const files: string[] = [];
        const directories: string[] = [];

        for (const fileOrDirName of filesOrDirectories) {
            const path = directoryPath + "/" + fileOrDirName;
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
