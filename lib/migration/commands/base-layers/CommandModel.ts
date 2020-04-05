import {Model, Types} from "model-layer";

export class CommandModel<Child extends CommandModel = any> extends Model<CommandModel & Child> {
    structure() {
        return {
            type: Types.String({
                required: true,
                enum: ["create", "drop"]
            }),
            command: Types.String({
                required: true,
                default: () => this.constructor.name.replace(/(Command)?Model$/, "")
            })
        };
    }
}
