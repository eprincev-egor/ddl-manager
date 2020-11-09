import { DatabaseFunction } from "../../ast";
import {
    wrapText
} from "./wrapText";

export function getUnfreezeFunctionSql(func: DatabaseFunction) {
    let prefix = "";
    if ( func.comment ) {
        prefix = func.comment + "\n";
    }

    const funcIdentifySql = func.getSignature();
    return `
        comment on function ${ funcIdentifySql } is ${wrapText( prefix + "ddl-manager-sync" )}
    `;
}