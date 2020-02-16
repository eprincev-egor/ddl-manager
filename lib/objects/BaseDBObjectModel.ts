import {Model, Types} from "model-layer";

export default abstract class BaseDBObjectModel<
    Child extends BaseDBObjectModel<any>
> extends Model<Child> {
    abstract getIdentify(): string;
}
