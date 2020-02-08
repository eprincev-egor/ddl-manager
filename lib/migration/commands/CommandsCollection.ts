import {Collection} from "model-layer";
import CommandModel from "./CommandModel";
import FunctionCommandModel from "./FunctionCommandModel";
import ViewCommandModel from "./ViewCommandModel";
import TableCommandModel from "./TableCommandModel";
import ColumnCommandModel from "./ColumnCommandModel";

export default class CommandsCollection extends Collection<CommandsCollection> {
    Model(): (
        (new (...args: any[]) => FunctionCommandModel) |
        (new (...args: any[]) => ViewCommandModel) |
        (new (...args: any[]) => TableCommandModel) |
        (new (...args: any[]) => ColumnCommandModel)
    )  {
        return CommandModel as any;
    }
}