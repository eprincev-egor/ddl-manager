import { Model, Types } from "model-layer";

export default class MigrationErrorModel<ChildError extends MigrationErrorModel<any>> 
extends Model<ChildError> {
    structure() {
        return {
            code: Types.String({
                required: true,
                default: () => this.constructor.name
            }),
            message: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]): string {
        throw new Error("method generateMessage not defined");
    }

    prepare(row: this["TInputData"]) {
        row.message = this.generateMessage(row);
    }
}