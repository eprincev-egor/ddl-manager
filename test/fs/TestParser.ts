import Parser from "../../lib/parser/Parser"
import BaseDBObjectModel from "../../lib/objects/BaseDBObjectModel";

export interface IState {
    [key: string]: BaseDBObjectModel<any>[];
}

export default class TestParser extends Parser {
    state: IState = {};

    constructor(state: IState) {
        super();
        this.state = state;
    }

    setTestFile(filePath: string, fileContent: string, objects: BaseDBObjectModel<any>[]) {
        filePath = normalizeFilePath(filePath);
        this.state[ filePath ] = objects;
    }

    removeTestFile(filePath: string) {
        filePath = normalizeFilePath(filePath);
        delete this.state[ filePath ];
    }

    parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[] {
        filePath = normalizeFilePath(filePath);
        return this.state[ filePath ];
    }
}

function normalizeDirPath(dirPath: string): string {
    dirPath = dirPath.replace(/^\.\//, "");
    dirPath = dirPath.replace(/\\/g, "/");
    dirPath = dirPath.replace(/\/$/, "");
    return dirPath;
}

function normalizeFilePath(filePath: string): string {
    filePath = normalizeDirPath(filePath);
    return filePath;
}