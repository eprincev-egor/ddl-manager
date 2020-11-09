import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface SetSelectItemRow {
    columns: string[];
    select: string;
}

export class SetSelectItem extends AbstractAstElement {

    readonly columns!: string[];
    readonly select!: string;

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
            this.printSelect(
                spaces.plusOneLevel()
            )
        );
        lines.push(spaces + ")");

        return lines;
    }

    private printSelect(spaces: Spaces) {
        let select = this.select;

        // TODO: use parsed/Select

        select = select.replace(
            /select\s+/, 
            "select\n" + spaces.plusOneLevel()
        );
        select = select.replace(
            / as (\w+),\s*/, 
            " as $1,\n" + spaces.plusOneLevel()
        );
        select = select.replace(
            /\s+from\s+/,
            `\n\n${spaces}from `
        );
        select = select.replace(
            /\s+(\w+) join\s+/g,
            `\n\n${spaces}$1 join `
        );
        select = select.replace(
            /\s+on\s+/g,
            ` on\n${spaces.plusOneLevel()}`
        );
        select = select.replace(
            /\s+where\s+/,
            `\n\n${spaces}where\n${spaces.plusOneLevel()}`
        );

        select = select.replace(/[ ]+$/mg, "");

        return spaces + select;
    }
}