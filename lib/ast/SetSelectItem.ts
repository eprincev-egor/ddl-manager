import { AbstractAstElement } from "./AbstractAstElement";
import { Select } from "./Select";
import { Spaces } from "./Spaces";

interface SetSelectItemRow {
    columns: string[];
    select: Select;
}

export class SetSelectItem extends AbstractAstElement {

    readonly columns!: string[];
    readonly select!: Select;

    constructor(row: SetSelectItemRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const lines: string[] = [
            spaces + "("
        ];

        for (let i = 0, n = this.columns.length; i < n; i++) {
            const columnName = this.columns[i];
            const line = spaces.plusOneLevel() + columnName;
            
            if ( i < n - 1 ) {
                lines.push(line + ",");
            }
            else {
                lines.push(line);
            }
        }
        
        lines.push(spaces + ") = (");
        lines.push(
            ...this.select.template(
                spaces.plusOneLevel()
            )
        );
        lines.push(spaces + ")");

        return lines;
    }
}