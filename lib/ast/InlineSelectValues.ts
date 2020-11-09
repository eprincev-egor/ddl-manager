import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface InlineSelectValuesRow {
    values: string[];
    union?: InlineSelectValues;
}

export class InlineSelectValues extends AbstractAstElement {

    readonly values!: string[];
    readonly union?: InlineSelectValues;

    constructor(row: InlineSelectValuesRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const lines = [
            spaces + "select " + this.values.join(", ")
        ];
        if ( this.union ) {
            lines.push(spaces + "union");
            lines.push( this.union.toSQL(spaces) );
        }

        return lines;
    }
}