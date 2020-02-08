import BaseDBObjectCollection from "./BaseDBObjectCollection";
import ViewModel from "./ViewModel";

export default class ViewsCollection extends BaseDBObjectCollection<ViewsCollection> {
    Model() {
        return ViewModel;
    }
}