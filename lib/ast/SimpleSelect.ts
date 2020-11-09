import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface SimpleSelectRow {
    column: string;
    from: string;
    where: string;
}

export class SimpleSelect extends AbstractAstElement {

    readonly column!: string;
    readonly from!: string;
    readonly where!: string;

    constructor(row: SimpleSelectRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        return [
            spaces + "(",
            spaces.plusOneLevel() + "select",
            spaces.plusOneLevel().plusOneLevel() + `${this.from}.${this.column}`,
            spaces.plusOneLevel() + `from ${this.from}`,
            spaces.plusOneLevel() + "where",
            spaces.plusOneLevel().plusOneLevel() + `${this.from}.id = ${this.where}`,
            spaces + ")"
        ];
    }
}