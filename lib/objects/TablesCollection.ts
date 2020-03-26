import {BaseDBObjectCollection} from "./base-layers/BaseDBObjectCollection";
import {TableModel} from "./TableModel";

export class TablesCollection extends BaseDBObjectCollection<TablesCollection> {
    Model() {
        return TableModel;
    }
}