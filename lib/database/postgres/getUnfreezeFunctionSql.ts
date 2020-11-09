import {
    wrapText
} from "../../utils";

export function getUnfreezeFunctionSql(func: any) {
    let prefix = "";
    if ( func.comment ) {
        prefix = func.comment + "\n";
    }

    const funcIdentifySql = func.getSignature();
    return `
        comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}