import { 
    ColumnDefinition
} from "grapeql-lang";
import { ColumnDBO } from "../../objects/ColumnDBO";


export function parseColumns(
    tableIdentify: string, 
    gqlColumns: ColumnDefinition[]
) {
    return gqlColumns.map(gqlColumn => {

        const columnDBO = new ColumnDBO({
            table: tableIdentify,
            name: gqlColumn.get("name").toString(),
            type: gqlColumn.get("type").toString(),
            nulls: gqlColumn.get("nulls"),
            default: gqlColumn.get("default").toString()
        });

        return columnDBO;
    });
}
