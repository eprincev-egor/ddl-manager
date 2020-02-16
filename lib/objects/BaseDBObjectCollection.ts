import {Collection} from "model-layer";
import BaseDBObjectModel from "./BaseDBObjectModel";

interface IChildCollection {
    Model(): (new (...args: any[]) => BaseDBObjectModel<any>);
}

export default abstract class BaseDBObjectCollection<
    ChildCollection extends BaseDBObjectCollection<any> & IChildCollection
> extends Collection<ChildCollection> {
    
    getByIdentify(identify: string): this["TModel"] {
        const existsModel = this.find((model) =>
            model.getIdentify() === identify
        );

        return existsModel;
    }
}