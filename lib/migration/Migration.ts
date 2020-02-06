import {Model, Types} from "model-layer";
import CommandsCollection from "./CommandsCollection";

export default class Migration extends Model<Migration> {
    structure() {
        return {
            commands: Types.Collection({
                Collection: CommandsCollection,
                default: () => new CommandsCollection()
            })
        };
    }
}