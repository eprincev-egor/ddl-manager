import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class ReferenceToUnknownColumnErrorModel 
extends MigrationErrorModel<ReferenceToUnknownColumnErrorModel> {
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
            }),
            referenceColumns: Types.Array({
                element: Types.String({
                    required: true
                }),
                required: true,
                unique: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        const unknownColumnsText = row.referenceColumns.map(key => `'${key}'`).join(", ");
        
        return (
            `foreign key '${row.foreignKeyName}' on table '${row.tableIdentify}'` +
            ` reference to unknown columns ${unknownColumnsText}` +
            ` in table '${row.referenceTableIdentify}'`
        );
    }
}