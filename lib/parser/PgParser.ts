import BaseDBObjectModel from "../objects/BaseDBObjectModel";
import FunctionModel from "../objects/FunctionModel";
import Parser from "./Parser";
import {
    GrapeQLCoach,
    CreateFunction
} from "grapeql-lang";

export default class PgParser extends Parser {
    parseFile(fileContent: string): BaseDBObjectModel<any>[] {
        const coach = new GrapeQLCoach(fileContent);
        const objects: BaseDBObjectModel<any>[] = [];

        for (let i = 0, n = coach.str.length; i < n; i++) {
            if ( coach.is(CreateFunction) ) {
                const parsedFunction = coach.parse(CreateFunction);
                const funcModel = new FunctionModel({
                    schema: parsedFunction.get("schema"),
                    name: parsedFunction.get("name"),
                    args: parsedFunction.get("args").join(",")
                });
                
                objects.push(funcModel);
            }
        }

        return objects;
    }
}