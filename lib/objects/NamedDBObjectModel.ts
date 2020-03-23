import {Types} from "model-layer";
import BaseDBObjectModel from "./BaseDBObjectModel";

const MAX_NAME_LENGTH = 64;

export default abstract class NamedDBObjectModel<
    Child extends NamedDBObjectModel<any>
> extends BaseDBObjectModel<Child> {
    structure() {
        return {
            ...super.structure(),
            name: Types.String({
                required: true
            })
        };
    }

    isValidNameLength() {
        return this.row.name.length <= MAX_NAME_LENGTH;
    }
}
