import {
    wrapText,
    trigger2identifySql
} from "../../utils";

export function getUnfreezeTriggerSql(trigger: any) {
    let prefix = "";
    if ( trigger.comment ) {
        const comment = trigger.comment;

        if ( typeof comment === "string" ) {
            prefix = comment + "\n";
        }
        else if ( typeof comment.comment === "string" ) {
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