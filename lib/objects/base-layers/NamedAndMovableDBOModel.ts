
import {Types} from "model-layer";
import {NamedDBObjectModel} from "./NamedDBObjectModel";

export abstract class NamedAndMovableDBOModel<
    Child extends NamedAndMovableDBOModel<any>
> extends NamedDBObjectModel<Child> {
    structure() {
        return {
            ...super.structure(),
            createdByDDLManager: Types.Boolean({
                default: true,
                required: true
            })
        };
    }

    allowedToDrop() {
        return this.row.createdByDDLManager;
    }
}
