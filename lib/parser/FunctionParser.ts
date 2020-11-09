import {
    GrapeQLCoach,
    CreateFunction
} from "grapeql-lang";
import { DatabaseFunction } from "../ast/DatabaseFunction";

export class FunctionParser {

    parse(sql: string) {
        const coach = new GrapeQLCoach(sql);
        const funcSyntax = coach.parse(CreateFunction);
        const json = funcSyntax.toJSON();
        const func = new DatabaseFunction(json as any);
        return func;
    }
}