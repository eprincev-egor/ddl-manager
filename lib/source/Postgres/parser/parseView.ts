import {
    GrapeQLCoach,
    CreateView
} from "grapeql-lang";
import { ViewDBO } from "../objects/ViewDBO";

export function parseView(coach: GrapeQLCoach) {
    const gqlView = coach.parse(CreateView);

    const viewDBO = new ViewDBO({
        schema: gqlView.get("schema").toString(),
        name: gqlView.get("name").toString(),
        select: gqlView.get("select").toString()
    });

    return viewDBO;
}