import {MigrationErrorModel} from "./MigrationErrorModel";
import { Types } from "model-layer";

export class ExpectedPrimaryKeyForRowsErrorModel 
extends MigrationErrorModel<ExpectedPrimaryKeyForRowsErrorModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `table ${row.tableIdentify} should have primary key for creating rows`;
    }
}