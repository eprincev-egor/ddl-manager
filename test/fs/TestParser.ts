import Parser from "../../lib/parser/Parser"
import BaseDBObjectModel from "../../lib/objects/BaseDBObjectModel";

interface IState {
    [key: string]: BaseDBObjectModel<any>[];
}

export default class TestParser extends Parser {
    state: IState = {};

    constructor(state: IState) {
        super();
        this.state = state;
    }

    parseFile(filePath: string, fileContent: string) {
        return this.state[ fileContent ];
    }
}