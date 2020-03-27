import { NamedDBObjectModel } from "./base-layers/NamedDBObjectModel";

export class CheckConstraintModel 
extends NamedDBObjectModel<CheckConstraintModel> {
    allowedToDrop() {
        return true;
    }
}