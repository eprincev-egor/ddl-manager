import { Collection } from "model-layer";
import MigrationErrorModel from "./MigrationErrorModel";
import UnknownTableForTriggerErrorModel from "./UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "./UnknownFunctionForTriggerErrorModel";

export default class MigrationErrorsCollection extends Collection<MigrationErrorsCollection> {
    Model(): (
        (new (...args: any[]) => UnknownTableForTriggerErrorModel) |
        (new (...args: any[]) => UnknownFunctionForTriggerErrorModel)
    ) {
        return MigrationErrorModel as any;
    }
}