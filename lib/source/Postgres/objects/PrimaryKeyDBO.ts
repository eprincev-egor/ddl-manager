import { AbstractConstraintDBO } from "./AbstractConstraintDBO";
import { Types } from "model-layer";

export class PrimaryKeyDBO 
extends AbstractConstraintDBO<PrimaryKeyDBO> {
    structure() {
        return {
            ...super.structure(),
            primaryKey: Types.Array({
                element: Types.String
            })
        };
    }

    toCreateSQL() {
        const row = this.row;
        return `
            alter table ${row.table}
            add constraint ${row.name} 
            primary key (${ row.primaryKey })
        `;
    }
}