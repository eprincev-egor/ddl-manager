import { NamedDBObjectModel } from "./base-layers/NamedDBObjectModel";
import { Types } from "model-layer";

export class CheckConstraintModel 
extends NamedDBObjectModel<CheckConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            check: Types.String({
                required: true
            })
        };
    }
    
    allowedToDrop() {
        return true;
    }

    validate(row: this["row"]) {
        const isValidName = !/.*_not_null$/i.test(row.name);
        
        if ( !isValidName ) {
            throw new Error("invalid name: '" + row.name + "', name cannot contain '_not_null'");
        }
    }
}