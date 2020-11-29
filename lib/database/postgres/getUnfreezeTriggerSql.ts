import { DatabaseTrigger } from "../schema/DatabaseTrigger";
import {
    wrapText
} from "./wrapText";

export function getUnfreezeTriggerSql(trigger: DatabaseTrigger) {
    let prefix = "";
    if ( trigger.comment ) {
        prefix = trigger.comment + "\n";
    }

    const triggerIdentifySql = trigger.getSignature();
    return `
        comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}