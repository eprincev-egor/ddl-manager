import { TableModel } from "../../../lib/objects/TableModel";
import { ColumnModel } from "../../../lib/objects/ColumnModel";
import { ExtensionModel } from "../../../lib/objects/ExtensionModel";

export function column(
    key: string, 
    type: string,
    additionalProperties: ColumnModel["TInputData"] = {}
): ColumnModel["TInputData"] {
    return {
        identify: key,
        key,
        type,
        ...additionalProperties
    };
}

export const columnID = column("id", "integer");
export const columnNAME = column("name", "text");

export function table(
    tableName: string,
    ...columns: ColumnModel["TInputData"][]
): TableModel["TInputData"] {
    return {
        filePath: `${tableName}.sql`,
        identify: `public.${tableName}`,
        name: tableName,
        columns
    };
}

export function extension(
    extensionName: string, 
    forTableName: string,
    additionalProperties: ExtensionModel["TInputData"] = {}
): ExtensionModel["TInputData"] {
    return {
        filePath: `${extensionName}_for_${forTableName}.sql`,
        name: extensionName,
        identify: `extension ${extensionName} for public.${forTableName}`,
        forTableIdentify: `public.${forTableName}`,
        ...additionalProperties
    };
}
