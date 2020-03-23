import { Model, Types } from "model-layer";

export abstract class MigrationErrorModel<ChildError extends MigrationErrorModel<any>> 
extends Model<ChildError> {
    structure() {
        return {
            filePath: Types.String({
                required: true
            }),
            code: Types.String({
                required: true,
                default: () => this.constructor.name.replace(/Model$/, "")
            }),
            message: Types.String({
                required: true
            })
        };
    }

    abstract generateMessage(row: this["TInputData"]): string;

    prepare(row: this["TInputData"]) {
        row.message = this.generateMessage(row);
    }
}