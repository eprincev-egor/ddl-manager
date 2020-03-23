import {MigrationErrorModel} from "./MigrationErrorModel";
import { Types } from "model-layer";

export class MaxObjectNameSizeErrorModel 
extends MigrationErrorModel<MaxObjectNameSizeErrorModel> {
    structure() {
        return {
            ...super.structure(),
            
            objectType: Types.String({
                required: true
            }),
            name: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `${row.objectType} name too long: ${row.name}, max size is 64 symbols`;
    }
}