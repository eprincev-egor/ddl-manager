import { AbstractConstraintDBO } from "./AbstractConstraintDBO";
import { Types } from "model-layer";

export class UniqueConstraintDBO 
extends AbstractConstraintDBO<UniqueConstraintDBO> {
    structure() {
        return {
            ...super.structure(),
            unique: Types.Array({
                element: Types.String
            })
        };
    }

    toCreateSQL() {
        const row = this.row;
        return `
            alter table ${row.table}
            add constraint ${row.name} 
            unique (${ row.unique })
        `;
    }
}