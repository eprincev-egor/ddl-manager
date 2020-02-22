import {Collection} from "model-layer";
import CommandModel from "./CommandModel";
import FunctionCommandModel from "./FunctionCommandModel";
import ViewCommandModel from "./ViewCommandModel";
import TableCommandModel from "./TableCommandModel";
import ColumnCommandModel from "./ColumnCommandModel";
import TriggerCommandModel from "./TriggerCommandModel";
import RowsCommandModel from "./RowsCommandModel";
import ColumnNotNullCommandModel from "./ColumnNotNullCommandModel";
import PrimaryKeyCommandModel from "./PrimaryKeyCommandModel";
import CheckConstraintCommandModel from "./CheckConstraintCommandModel";

export default class CommandsCollection extends Collection<CommandsCollection> {
    Model(): (
        (new (...args: any[]) => CheckConstraintCommandModel) |
        (new (...args: any[]) => PrimaryKeyCommandModel) |
        (new (...args: any[]) => ColumnNotNullCommandModel) |
        (new (...args: any[]) => RowsCommandModel) |
        (new (...args: any[]) => FunctionCommandModel) |
        (new (...args: any[]) => ViewCommandModel) |
        (new (...args: any[]) => TableCommandModel) |
        (new (...args: any[]) => ColumnCommandModel) |
        (new (...args: any[]) => TriggerCommandModel)
    )  {
        return CommandModel as any;
    }
}