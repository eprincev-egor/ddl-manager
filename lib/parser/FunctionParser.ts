import {
    GrapeQLCoach,
    CreateFunction
} from "grapeql-lang";

export class FunctionParser {

    parse(sql: string) {
        const coach = new GrapeQLCoach(sql);
        const func = coach.parse(CreateFunction);
        return func.toJSON();
    }
}