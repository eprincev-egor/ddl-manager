import {Collection, Model, Types} from "model-layer";

export class TriggerModel extends Model<TriggerModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String
        };
    }
}

export class TriggersCollection extends Collection<TriggerModel> {
    Model() {return TriggerModel;}
}