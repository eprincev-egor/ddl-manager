import {Collection} from "model-layer";
import CommandModel from "./CommandModel";

export default class CommandsCollection extends Collection<CommandsCollection> {
    Model() {
        return CommandModel;
    }
}