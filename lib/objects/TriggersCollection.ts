import {BaseDBObjectCollection} from "./base-layers/BaseDBObjectCollection";
import {TriggerModel} from "./TriggerModel";

export class TriggersCollection extends BaseDBObjectCollection<TriggersCollection> {
    Model() {
        return TriggerModel;
    }
}