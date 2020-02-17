import { Collection } from "model-layer";
import MigrationErrorModel from "./MigrationErrorModel";
import UnknownTableForTriggerErrorModel from "./UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "./UnknownFunctionForTriggerErrorModel";
import MaxObjectNameSizeErrorModel from "./MaxObjectNameSizeErrorModel";

export default class MigrationErrorsCollection extends Collection<MigrationErrorsCollection> {
    Model(): (
        (new (...args: any[]) => MaxObjectNameSizeErrorModel) |
        (new (...args: any[]) => UnknownTableForTriggerErrorModel) |
        (new (...args: any[]) => UnknownFunctionForTriggerErrorModel)
    ) {
        return MigrationErrorModel as any;
    }
}