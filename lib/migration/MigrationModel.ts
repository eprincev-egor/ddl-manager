import {Model, Types} from "model-layer";
import {CommandsCollection} from "./commands/base-layers/CommandsCollection";
import {MigrationErrorsCollection} from "./errors/base-layers/MigrationErrorsCollection";

export type InputCommand = (
    CommandsCollection["TModel"]
);
export type InputError = (
    MigrationErrorsCollection["TModel"]
);
export class MigrationModel extends Model<MigrationModel> {
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