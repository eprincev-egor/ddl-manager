import { NamedDBObjectModel } from "./base-layers/NamedDBObjectModel";

export class CheckConstraintModel 
extends NamedDBObjectModel<CheckConstraintModel> {
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