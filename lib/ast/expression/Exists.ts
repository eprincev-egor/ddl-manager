import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Select } from "../Select";
import { Spaces } from "../Spaces";

interface ExistsRow {
    select: Select;
}

export class Exists extends AbstractExpressionElement {

    readonly select: Select;

    constructor(row: ExistsRow) {
        super();
        this.select = row.select;
    }

    getColumnReferences() {
        return this.select.getAllColumnReferences();
    }

    clone() {
        return new Exists({
            select: this.select.cloneWith({})
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