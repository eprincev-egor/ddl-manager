import {Collection} from "model-layer";
import BaseDBObjectModel, {IChildObjectModel} from "./BaseDBObjectModel";

export default class BaseDBObjectCollection<
    ChildModel extends BaseDBObjectModel<any> & 
    IChildObjectModel
> extends Collection<ChildModel> {
    
    getByIdentify(identify: string): ChildModel {
        const existsModel = this.find(model =>
            model.getIdentify() === identify
        );

        return existsModel;
    }
}