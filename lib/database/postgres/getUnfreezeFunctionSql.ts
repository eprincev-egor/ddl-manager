import {
    wrapText,
    function2identifySql
} from "../../utils";

export function getUnfreezeFunctionSql(func: any, comment: any) {
    let prefix = "";
    if ( comment ) {
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

    const funcIdentifySql = function2identifySql( func );
    return `
        comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}