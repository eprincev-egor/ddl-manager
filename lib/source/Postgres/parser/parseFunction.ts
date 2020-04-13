import {
    GrapeQLCoach,
    CreateFunction
} from "grapeql-lang";
import { FunctionDBO } from "../objects/FunctionDBO";

export function parseFunction(coach: GrapeQLCoach): FunctionDBO {
    const gqlFunction = coach.parse(CreateFunction);
    const functionDBO = new FunctionDBO({
        schema: gqlFunction.get("schema"),
        name: gqlFunction.get("name"),
        args: gqlFunction.get("args").map(arg => arg.toString()),
        returns: gqlFunction.get("returns").toString(),
        body: gqlFunction.get("body").toString(),
        language: gqlFunction.get("language"),
        immutable: gqlFunction.get("immutable"),
        returnsNullOnNull: gqlFunction.get("returnsNullOnNull"),
        stable: gqlFunction.get("stable"),
        strict: gqlFunction.get("strict"),
        parallel: gqlFunction.get("parallel"),
        cost: gqlFunction.get("cost")
    });

    return functionDBO;
}