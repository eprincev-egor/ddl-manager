import {Types} from "model-layer";
import NamedAndMovableDBOModel from "./NamedAndMovableDBOModel";

export default 
class TriggerModel 
extends NamedAndMovableDBOModel<TriggerModel> {
    structure() {
        return {
            ...super.structure(),
            
            tableIdentify: Types.String({
                required: true
            }),
            functionIdentify: Types.String({
                required: true
            })
        };
    }
}