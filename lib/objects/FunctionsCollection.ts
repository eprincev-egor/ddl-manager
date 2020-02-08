import BaseDBObjectCollection from "./BaseDBObjectCollection";
import FunctionModel from "./FunctionModel";

export default class FunctionsCollection extends BaseDBObjectCollection<FunctionsCollection> {
    Model() {
        return FunctionModel;
    }
}