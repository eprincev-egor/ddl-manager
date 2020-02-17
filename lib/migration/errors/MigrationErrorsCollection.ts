import { Collection } from "model-layer";
import MigrationErrorModel from "./MigrationErrorModel";
import UnknownTableForTriggerErrorModel from "./UnknownTableForTriggerErrorModel";

export default class MigrationErrorsCollection extends Collection<MigrationErrorsCollection> {
    Model(): (
        (new (...args: any[]) => UnknownTableForTriggerErrorModel)
    ) {
        return MigrationErrorModel as any;
    }
}