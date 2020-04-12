import { AbstractConstraintDBO } from "./AbstractConstraintDBO";
import { Types } from "model-layer";

export class ForeignKeyDBO
extends AbstractConstraintDBO<ForeignKeyDBO> {
    structure() {
        return {
            ...super.structure(),
            columns: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            }),
            referenceTable: Types.String({
                required: true
            }),
            referenceColumns: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            }),
            match: Types.String,
            onDelete: Types.String,
            onUpdate: Types.String
        };
    }

    toCreateSQL() {
        const row = this.row;
        const optionsSQL = this.getOptionsSQL();
        return `
            alter table ${row.table}
            add constraint ${row.name} 
            foreign key (${ row.columns })
            references ${row.referenceTable} (${ row.referenceColumns })
            ${ optionsSQL }
        `;
    }

    private getOptionsSQL() {
        let optionsSQL = "";
        const {match, onDelete, onUpdate} = this.row;
        
        if ( match ) {
            optionsSQL += " match ";
            optionsSQL += match;
        }
        
        if ( onDelete ) {
            optionsSQL += " on delete ";
            optionsSQL += onDelete;
        }

        if ( onUpdate ) {
            optionsSQL += " on update ";
            optionsSQL += onUpdate;
        }

        return optionsSQL;
    }
}