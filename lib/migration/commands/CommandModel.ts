import {Model, Types} from "model-layer";

export default class CommandModel<Child extends CommandModel = any> extends Model<CommandModel & Child> {
    structure() {
        return {
            type: Types.String({
                required: true
            })
        };
    }
}
