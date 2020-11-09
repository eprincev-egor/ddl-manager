import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface SetItemRow {
    column: string;
    value: AbstractAstElement;
}

export class SetItem extends AbstractAstElement {

    readonly column!: string;
    readonly value!: AbstractAstElement;

    constructor(row: SetItemRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const valueSQL = this.value
            .toSQL( spaces )
            .trim();
        return [
            spaces + `${ this.column } = ${ valueSQL }`
        ];
    }
}