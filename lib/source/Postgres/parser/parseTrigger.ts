import {
    GrapeQLCoach,
    CreateTrigger
} from "grapeql-lang";
import { TriggerDBO } from "../objects/TriggerDBO";

export function parseTrigger(coach: GrapeQLCoach) {
    const gqlTrigger = coach.parse(CreateTrigger);

    const triggerDBO = new TriggerDBO({
        name: gqlTrigger.get("name"),
        table: gqlTrigger.get("table").toString(),
        procedure: gqlTrigger.get("procedure").toString(),
        events: parseTriggerEvents(gqlTrigger),

        constraint: gqlTrigger.get("constraint"),
        deferrable: gqlTrigger.get("deferrable"),
        statement: gqlTrigger.get("statement"),
        initially: gqlTrigger.get("initially"),
        when: gqlTrigger.get("when")
    });

    return triggerDBO;
}

function parseTriggerEvents(triggerModel: CreateTrigger) {
    const trigger = triggerModel.row;
    let triggerEvents = "";

    // after|before
    if ( trigger.before ) {
        triggerEvents += "before";
    }
    else {
        triggerEvents += "after";
    }
    triggerEvents += " ";

    // insert or update of x or delete
    const tableEvents = [];
    if ( trigger.insert ) {
        tableEvents.push("insert");
    }
    if ( trigger.update ) {
        if ( trigger.updateOf ) {
            tableEvents.push(`update of ${ trigger.updateOf.join(", ") }`);
        } else {
            tableEvents.push("update");
        }
    }
    if ( trigger.delete ) {
        tableEvents.push("delete");
    }
    triggerEvents += tableEvents.join(" or ");

    return triggerEvents;
}