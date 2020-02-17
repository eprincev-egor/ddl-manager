import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class UnknownFunctionForTriggerErrorModel 
extends MigrationErrorModel<UnknownFunctionForTriggerErrorModel> {
    structure() {
        return {
            ...super.structure(),

            functionIdentify: Types.String({
                required: true
            }),
            triggerName: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `not found function ${row.functionIdentify} for trigger ${row.triggerName}`;
    }
}