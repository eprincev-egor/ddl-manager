import {BaseDBObjectModel} from "../objects/base-layers/BaseDBObjectModel";

export abstract class Parser {
    abstract parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[];
}
