import {Model, Types} from "model-layer";

export default abstract class BaseDBObjectModel<
    Child extends BaseDBObjectModel<any>
> extends Model<Child> {
    structure() {
        return {
            identify: Types.String({
                required: true
            }),
            parsed: Types.Any,
            filePath: Types.String
        };
    }

    getIdentify() {
        return this.row.identify;
    }
}
