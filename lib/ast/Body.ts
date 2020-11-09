import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";
import { Declare } from "./Declare";

interface BodyRow {
    declares?: Declare[];
    statements: AbstractAstElement[];
}

export class Body extends AbstractAstElement {

    readonly declares?: Declare[];
    readonly statements!: AbstractAstElement[];

    constructor(row: BodyRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        return [
            ...(this.declares || []).map(declare =>
                declare.toSQL()
            ),
            "begin",
            ...this.statements.map(statement => 
                statement.toSQL( spaces.plusOneLevel() )
            ),
            "end"
        ];
    }
}