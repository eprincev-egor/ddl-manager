import { TableModel } from "../../../lib/objects/TableModel";
import { ColumnModel } from "../../../lib/objects/ColumnModel";
import { ExtensionModel } from "../../../lib/objects/ExtensionModel";

export const columnID: ColumnModel["TInputData"] = {
    identify: "id",
    key: "id",
    type: "integer"
};

export const columnNAME: ColumnModel["TInputData"] = {
    identify: "name",
    key: "name",
    type: "text"
};

export const columnINN: ColumnModel["TInputData"] = {
    identify: "inn",
    key: "inn",
    type: "text"
};

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
    forTableName: string
): ExtensionModel["TInputData"] {
    return {
        filePath: `${extensionName}_for_${forTableName}.sql`,
        name: extensionName,
        identify: `extension ${extensionName} for public.${forTableName}`,
        forTableIdentify: `public.${forTableName}`
    };
}
