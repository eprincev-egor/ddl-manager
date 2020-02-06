import BaseDBObjectCollection from "./BaseDBObjectCollection";
import FunctionModel from "./FunctionModel";

export default class FunctionsCollection extends BaseDBObjectCollection<FunctionModel> {
    Model() {
        return FunctionModel;
    }
}