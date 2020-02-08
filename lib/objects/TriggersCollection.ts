import BaseDBObjectCollection from "./BaseDBObjectCollection";
import TriggerModel from "./TriggerModel";

export default class TriggersCollection extends BaseDBObjectCollection<TriggersCollection> {
    Model() {
        return TriggerModel;
    }
}