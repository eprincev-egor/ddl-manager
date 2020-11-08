import {
    wrapText,
    trigger2identifySql
} from "../../utils";

export function getUnfreezeTriggerSql(trigger: any, comment: any) {
    let prefix = "";
    if ( comment ) {
        if ( typeof comment.comment === "string" ) {
            prefix = comment.comment + "\n";
        }
        else if ( comment.comment.content && typeof comment.comment.content === "string" ) {
            prefix = comment.comment.content + "\n";
        }
    }

    const triggerIdentifySql = trigger2identifySql( trigger );
    return `
        comment on trigger ${ triggerIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}