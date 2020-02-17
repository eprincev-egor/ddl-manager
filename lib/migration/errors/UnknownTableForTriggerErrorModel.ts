import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class UnknownTableForTriggerErrorModel 
extends MigrationErrorModel<UnknownTableForTriggerErrorModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            triggerName: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `not found table ${row.tableIdentify} for trigger ${row.triggerName}`;
    }
}