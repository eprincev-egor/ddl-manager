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

        for (; coach.i < coach.str.length; coach.i++) {
            if ( coach.is(CreateFunction) ) {
                const parsedFunction = coach.parse(CreateFunction);
                const {schema, name, args} = parsedFunction.row;
                const functionIdentify = `${schema}.${name}(${args.join(",")})`;

                const funcModel = new FunctionModel({
                    identify: functionIdentify,
                    name: parsedFunction.get("name"),
                    parsed: parsedFunction
                });
                
                objects.push(funcModel);
            }
        }

        return objects;
    }
}