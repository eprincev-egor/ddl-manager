import {Model, Types} from "model-layer";
import {BaseDBObjectModel} from "../objects/BaseDBObjectModel";

export class FileModel extends Model<FileModel> {
    structure() {
        return {
            name: Types.String({
                required: true
            }),
            path: Types.String({
                required: true
            }),
            content: Types.String({
                required: true
            }),
            objects: Types.Array({
                required: true,
                element: BaseDBObjectModel as any as (
                    new (...args: any) => BaseDBObjectModel<any>
                )
            })
        };
    }
}