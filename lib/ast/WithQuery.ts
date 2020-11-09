import { AbstractAstElement } from "./AbstractAstElement";
import { InlineSelectValues } from "./InlineSelectValues";
import { Spaces } from "./Spaces";

interface WithQueryRow {
    name: string;
    // another selects?
    select: InlineSelectValues
}

export class WithQuery extends AbstractAstElement {
    readonly name!: string;
    // another selects?
    readonly select!: InlineSelectValues

    constructor(row: WithQueryRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        return [
            spaces + `${this.name} as (`,
            this.select.toSQL( spaces.plusOneLevel() ),
            spaces + ")"
        ];
    }
}