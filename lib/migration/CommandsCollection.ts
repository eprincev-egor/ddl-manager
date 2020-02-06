import {Collection} from "model-layer";
import CommandModel from "./commands/CommandModel";

export default class CommandsCollection extends Collection<CommandModel> {
    Model() {
        return CommandModel;
    }
}