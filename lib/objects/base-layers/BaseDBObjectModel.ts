import {Model, Types} from "model-layer";

export abstract class BaseDBObjectModel<
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

    getTypeName() {
        const className = this.constructor.name;
        const typeName = className.replace(/Model$/, "")
            .toLowerCase();
        return typeName;
    }
}
