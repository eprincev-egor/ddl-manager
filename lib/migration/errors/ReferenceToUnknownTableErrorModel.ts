import {MigrationErrorModel} from "./base-layers/MigrationErrorModel";
import { Types } from "model-layer";

export class ReferenceToUnknownTableErrorModel 
extends MigrationErrorModel<ReferenceToUnknownTableErrorModel> {
    structure() {
        return {
            ...super.structure(),

            foreignKeyName: Types.String({
                required: true
            }),
            tableIdentify: Types.String({
                required: true
            }),
            referenceTableIdentify: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `foreign key '${row.foreignKeyName}' on table '${row.tableIdentify}' reference to unknown table '${row.referenceTableIdentify}'`;
    }
}