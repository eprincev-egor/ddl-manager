import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";
import { WithQuery } from "./WithQuery";

interface WithRow {
    queries: WithQuery[];
}

export class With extends AbstractAstElement {

    readonly queries!: WithQuery[];

    constructor(row: WithRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        return [
            spaces + "with",
            ...this.queries.map(query =>
                query.toSQL( spaces.plusOneLevel() )
            )
        ];
    }
}