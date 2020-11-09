import {
    wrapText
} from "../../utils";

export function getUnfreezeTriggerSql(trigger: any) {
    let prefix = "";
    if ( trigger.comment ) {
        prefix = trigger.comment + "\n";
    }

    const triggerIdentifySql = trigger.getSignature();
    return `
        comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}