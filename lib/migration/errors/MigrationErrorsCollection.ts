import { Collection } from "model-layer";
import MigrationErrorModel from "./MigrationErrorModel";
import UnknownTableForTriggerErrorModel from "./UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "./UnknownFunctionForTriggerErrorModel";
import MaxObjectNameSizeErrorModel from "./MaxObjectNameSizeErrorModel";
import CannotDropColumnErrorModel from "./CannotDropColumnErrorModel";
import CannotDropTableErrorModel from "./CannotDropTableErrorModel";

export default class MigrationErrorsCollection extends Collection<MigrationErrorsCollection> {
    Model(): (
        (new (...args: any[]) => CannotDropTableErrorModel) |
        (new (...args: any[]) => CannotDropColumnErrorModel) |
        (new (...args: any[]) => MaxObjectNameSizeErrorModel) |
        (new (...args: any[]) => UnknownTableForTriggerErrorModel) |
        (new (...args: any[]) => UnknownFunctionForTriggerErrorModel)
    ) {
        return MigrationErrorModel as any;
    }
}