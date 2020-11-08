import {
    wrapText,
    trigger2identifySql
} from "../../utils";

export function getUnfreezeTriggerSql(trigger: any) {
    let prefix = "";
    if ( trigger.comment ) {
        prefix = trigger.comment + "\n";
    }

    const triggerIdentifySql = trigger2identifySql( trigger );
    return `
        comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}