import {Model, Types} from "model-layer";
import CommandsCollection from "./commands/CommandsCollection";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";

type InputCommand = (
    CommandsCollection["TModel"]
);
type InputError = (
    MigrationErrorsCollection["TModel"]
);
export default class MigrationModel extends Model<MigrationModel> {
    structure() {
        return {
            commands: Types.Collection({
                Collection: CommandsCollection,
                default: () => new CommandsCollection()
            }),
            errors: Types.Collection({
                Collection: MigrationErrorsCollection,
                default: () => new MigrationErrorsCollection()
            })
        };
    }

    addCommand(command: InputCommand) {
        this.row.commands.push(command);
    }

    addError(error: InputError) {
        this.row.errors.push(error);
    }
}