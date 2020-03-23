import {BaseDBObjectCollection} from "./BaseDBObjectCollection";
import {ViewModel} from "./ViewModel";

export class ViewsCollection extends BaseDBObjectCollection<ViewsCollection> {
    Model() {
        return ViewModel;
    }
}