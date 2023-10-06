import {
    TableReference as TableLink,
    Name as ObjectName
} from "psql-lang";
import { TableID } from "../database/schema/TableID";
import { DEFAULT_SCHEMA } from "./defaults";
import { TableReference } from "../database/schema/TableReference";

export function parseFromTable(
    tableLink: TableLink,
    alias?: ObjectName
) {
    return new TableReference(
        new TableID(
            tableLink.row.schema?.toValue() || DEFAULT_SCHEMA,
            tableLink.row.name.toValue(),
        ),
        alias?.toValue()
    );
}