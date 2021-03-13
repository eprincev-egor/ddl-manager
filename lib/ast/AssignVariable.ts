import { AbstractAstElement } from "./AbstractAstElement";
import { Exists } from "./expression/Exists";
import { HardCode } from "./HardCode";
import { SimpleSelect } from "./SimpleSelect";
import { Spaces } from "./Spaces";

interface AssignVariableRow {
    variable: string;
    value: HardCode | SimpleSelect | Exists;
}

export class AssignVariable extends AbstractAstElement {
    readonly variable!: string;
    readonly value!: HardCode | SimpleSelect;

    constructor(row: AssignVariableRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const valueSQL = this.value
            .toSQL( spaces )
            .trim();
        return [
            spaces + `${this.variable} = ${ valueSQL };`
        ];
    }
}