import {BaseDBObjectModel} from "../objects/BaseDBObjectModel";

export abstract class Parser {
    abstract parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[];
}
