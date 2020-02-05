import {Collection, Model, Types} from "model-layer";

export class ViewModel extends Model<ViewModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String
        };
    }
}

export class ViewsCollection extends Collection<ViewModel> {
    Model() {return ViewModel;}
}