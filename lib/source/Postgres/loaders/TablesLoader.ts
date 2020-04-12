import fs from "fs";
import { ILoader } from "./ILoader";
import { IDBDriver } from "../../../common";
import { TableDBO } from "../objects/TableDBO";
import { ColumnDBO } from "../objects/ColumnDBO";
import { ConstraintsLoader } from "./ConstraintsLoader";

const sqlColumnsPath = __dirname + "/sql/select-columns.sql";
const selectColumnsSQL = fs.readFileSync(sqlColumnsPath).toString();

interface IColumnRow {
    table_identify: string;
    table_schema: string;
    table_name: string;
    column_name: string;
    column_default: string;
    data_type: string;
    is_nullable: "YES" | "NO" | null;
}

interface ITableByIdentify {
    [key: string]: TableDBO["TInputData"]
}

export class TablesLoader
implements ILoader {
    private driver: IDBDriver;
    private constraintsLoader: ConstraintsLoader;

    constructor(driver: IDBDriver) {
        this.driver = driver;
        this.constraintsLoader = new ConstraintsLoader(driver);
    }
    
    async load(): Promise<TableDBO[]> {
        
        const tableByIdentify: ITableByIdentify = {};

        await this.loadColumns(tableByIdentify);
        await this.loadConstraints(tableByIdentify);

        const tables = Object.values(tableByIdentify).map(table => 
            new TableDBO(table)
        );
        return tables;
    }

    private async loadColumns(tableByIdentify: ITableByIdentify) {
        const columnsRows = await this.driver.query<IColumnRow>(selectColumnsSQL);
        
        for (const columnRow of columnsRows) {
            
            const tableIdentify = columnRow.table_identify;
            let table = tableByIdentify[ tableIdentify ];

            if ( !table ) {
                table = {
                    schema: columnRow.table_schema,
                    name: columnRow.table_name,
                    columns: [],
                    constraints: []
                };
                tableByIdentify[ tableIdentify ] = table;
            }
            
            const column = new ColumnDBO({
                name: columnRow.column_name,
                type: columnRow.data_type,
                default: columnRow.column_default,
                nulls: parseColumnNulls(columnRow)
            });
            table.columns.push(column);
        }
    }

    private async loadConstraints(tableByIdentify: ITableByIdentify) {
        const constraints = await this.constraintsLoader.load();

        for (const constraint of constraints) {
            const tableIdentify = constraint.row.table;
            const table = tableByIdentify[ tableIdentify ];

            table.constraints.push(constraint);
        }
    }
}

function parseColumnNulls(columnRow: IColumnRow) {
    if ( columnRow.is_nullable === "YES" ) {
        return true;
    } else {
        return false;
    }
}