import BaseDBObjectModel from "../objects/BaseDBObjectModel";

export default abstract class Parser {
    abstract parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[];
}
