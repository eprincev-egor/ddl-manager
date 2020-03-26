import {BaseDBObjectCollection} from "./base-layers/BaseDBObjectCollection";
import {FunctionModel} from "./FunctionModel";

export class FunctionsCollection extends BaseDBObjectCollection<FunctionsCollection> {
    Model() {
        return FunctionModel;
    }
}