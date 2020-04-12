import { AbstractConstraintDBO } from "./AbstractConstraintDBO";
import { Types } from "model-layer";

export class CheckConstraintDBO 
extends AbstractConstraintDBO<CheckConstraintDBO> {
    structure() {
        return {
            ...super.structure(),
            check: Types.String
        };
    }

    toCreateSQL() {
        const row = this.row;
        return `
            alter table ${row.table}
            add constraint ${row.name} 
            check (${ row.check })
        `;
    }
}