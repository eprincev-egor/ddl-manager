import { TableModel } from "../../../../lib/objects/TableModel";
import { ColumnModel } from "../../../../lib/objects/ColumnModel";

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

export const companiesWithId: TableModel["TInputData"] = {
    filePath: "companies.sql",
    identify: "public.companies",
    name: "companies",
    columns: [columnID]
};

export const companiesWithIdAndName: TableModel["TInputData"] = {
    filePath: "companies.sql",
    identify: "public.companies",
    name: "companies",
    columns: [
        columnID,
        columnNAME
    ]
};
