import { ColumnLink, ObjectName } from "grapeql-lang";
import { ColumnReference, TableReference, IReferenceFilter, Select } from "../ast";

export class ColumnReferenceParser {

    parse(
        select: Select, 
        // cacheFor, or "join" tableReference while he parsing
        additionalTableReferences: TableReference[], 
        columnLink: ColumnLink
    ) {
        const baseNames = columnLink.get("link") as any[];
        const tableNames = baseNames.slice(0, -1);

        let tableReference: TableReference | undefined;
        let refFilter!: IReferenceFilter;

        if ( tableNames.length === 1 ) {
            const aliasOrTableName = (tableNames[0] as ObjectName).toLowerCase() as string;

            refFilter = {aliasOrTableName};
        }
        else if ( tableNames.length === 2 ) {
            const schema = (tableNames[0] as ObjectName).toLowerCase() as string;
            const aliasOrTableName = (tableNames[1] as ObjectName).toLowerCase() as string;

            refFilter = {aliasOrTableName, schema};
        }
        else if ( tableNames.length === 0 ) {
            const allSources = select.getAllTableReferences();

            if ( allSources.length !== 1 ) {
                throw new Error(`required table for columnLink ${columnLink}, in select with not one source`);
            }

            tableReference = allSources[0];
        }
        else {
            throw new Error("invalid column link: " + columnLink);
        }


        if ( !tableReference ) {
            tableReference = select.findTableReference(refFilter);
        }
        if ( !tableReference ) {
            tableReference = additionalTableReferences.find(tableRef =>
                tableRef.matched(refFilter)
            );
        }

        if ( !tableReference ) {
            throw new Error(`source for column ${columnLink} not found`);
        }


        const columnNameSyntax = columnLink.last() as ObjectName;
        const columnName = columnNameSyntax.toLowerCase() as string;

        const columnReference = new ColumnReference(tableReference, columnName);
        return columnReference;
    }
}