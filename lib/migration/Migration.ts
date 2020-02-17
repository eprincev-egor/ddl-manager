import {Model, Types} from "model-layer";
import CommandsCollection from "./commands/CommandsCollection";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";

export default class Migration extends Model<Migration> {
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
}