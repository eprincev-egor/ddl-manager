import { Types } from "model-layer";
import { NamedDBObjectModel } from "./base-layers/NamedDBObjectModel";

export class UniqueConstraintModel 
extends NamedDBObjectModel<UniqueConstraintModel> {
    structure() {
        return {
            ...super.structure(),

            unique: Types.Array({
                element: Types.String,
                required: true,
                unique: true
            })
        };
    }

    allowedToDrop() {
        return true;
    }
}