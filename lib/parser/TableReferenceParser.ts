import { 
    ObjectName,
    TableLink
} from "grapeql-lang";
import { TableReference, Table } from "../ast";

const DEFAULT_SCHEMA = "public";

export class TableReferenceParser {
    parse(
        tableLink: TableLink,
        alias?: ObjectName
    ) {
        const link = tableLink.get("link") as ObjectName[];
        const output: {
            schema: string;
            table: string;
            alias?: string;
        } = {} as any;
    
        if ( link.length === 1 ) {
            const tableName = link[0].toLowerCase() as string;
            output.schema = DEFAULT_SCHEMA;
            output.table = tableName;
        }
        else if ( link.length === 2 ) {
            const schemaName = link[0].toLowerCase() as string;
            const tableName = link[1].toLowerCase() as string;
            
            output.schema = schemaName;
            output.table = tableName;
        }
        else {
            throw new Error(`invalid table ${ tableLink.toString() }`);
        }
    
        if ( alias ) {
            output.alias = alias.toLowerCase() as string;
        }
        else {
            output.alias = undefined;
        }
    
        const tableAlias = new TableReference(
            new Table(
                output.schema,
                output.table,
            ),
            output.alias
        );
        return tableAlias;
    }
}
