import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Select } from "../Select";
import { Spaces } from "../Spaces";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";

interface ExistsRow {
    select: Select;
}

export class Exists extends AbstractExpressionElement {

    readonly select: Select;

    constructor(row: ExistsRow) {
        super();
        this.select = row.select;
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new Exists({
            select: this.select.replaceTable(replaceTable, toTable)
        });
    }

    getColumnReferences() {
        return this.select.getAllColumnReferences();
    }

    clone() {
        return new Exists({
            select: this.select.clone({})
        });
    }

    template(spaces: Spaces) {
        const lines: string[] = [
            "exists(",
            this.select.toSQL(
                spaces.plusOneLevel()
            ),
            spaces + ")"
        ];
        
        return lines;
    }

}