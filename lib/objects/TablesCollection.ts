import BaseDBObjectCollection from "./BaseDBObjectCollection";
import TableModel from "./TableModel";

export default class TablesCollection extends BaseDBObjectCollection<TablesCollection> {
    Model() {
        return TableModel;
    }
}