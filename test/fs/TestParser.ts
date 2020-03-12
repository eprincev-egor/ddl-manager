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
        this.state[ filePath ] = objects;
    }

    parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[] {
        return this.state[ filePath ];
    }
}