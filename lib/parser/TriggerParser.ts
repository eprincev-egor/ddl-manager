import {
    GrapeQLCoach,
    CreateTrigger
} from "grapeql-lang";

export class TriggerParser {

    parse(sql: string) {
        const coach = new GrapeQLCoach(sql);
        const trigger = coach.parse(CreateTrigger);
        return trigger;
    }
}