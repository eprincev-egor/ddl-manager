import {Model, Types} from "model-layer";

// required methods/props
export interface IChildObjectModel {
    getIdentify: () => string;
    structure: () => any;
}

export default class BaseDBObjectModel<
    Child extends BaseDBObjectModel<any> & 
    IChildObjectModel
> extends Model<Child> {
    
}
