import {MigrationErrorModel} from "./base-layers/MigrationErrorModel";
import { Types } from "model-layer";

export class UnknownTableForExtensionErrorModel 
extends MigrationErrorModel<UnknownTableForExtensionErrorModel> {
    structure() {
        return {
            ...super.structure(),

            extensionName: Types.String({
                required: true
            }),
            tableIdentify: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `table ${row.tableIdentify} does not exists for extension ${row.extensionName}`;
    }
}