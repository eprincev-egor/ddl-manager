import {
    wrapText,
    function2identifySql
} from "../../utils";

export function getUnfreezeFunctionSql(func: any) {
    let prefix = "";
    if ( func.comment ) {
        prefix = func.comment + "\n";
    }

    const funcIdentifySql = function2identifySql( func );
    return `
        comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}