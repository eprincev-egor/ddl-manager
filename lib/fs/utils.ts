import fs from "fs";

export function formatPath(inputFilePath: string) {
    const filePaths = inputFilePath.split(/[/\\]/);
    const outputFilePath = filePaths.join("/");

    return outputFilePath;
}

export function isSqlFile(filePath: string) {
    if ( !/\.sql$/.test(filePath) ) {
        return false;
    }

    let stat;
    try {
        stat = fs.lstatSync(filePath);
    } catch(err) {
        return false;
    }

    return stat.isFile();
}


export function getSubPath(rootFolderPath: string, fullFilePath: string) {
    const subPath = fullFilePath.slice( rootFolderPath.length + 1 );
    return subPath;
}

export function prepareError(err: Error, subPath: string) {
    const outError = new Error(err.message) as any;
        
    outError.subPath = subPath;
    outError.originalError = err;

    return outError;
}