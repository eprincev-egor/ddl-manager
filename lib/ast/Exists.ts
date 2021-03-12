import { AbstractAstElement } from "./AbstractAstElement";
import { Select } from "./Select";
import { Spaces } from "./Spaces";

interface ExistsRow {
    select: Select;
}

export class Exists extends AbstractAstElement {

    readonly select: Select;

    constructor(row: ExistsRow) {
        super();
        this.select = row.select;
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