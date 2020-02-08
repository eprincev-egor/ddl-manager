import {Types} from "model-layer";
import CommandModel from "./CommandModel";
import ColumnModel from "../../objects/ColumnModel";
import { truncate } from "fs";

export default class ColumnCommandModel extends CommandModel<ColumnCommandModel> {
    structure() {
        return {
            ...super.structure(),

            schema: Types.String({
                required: true
            }),
            table: Types.String({
                required: true
            }),
            
            column: Types.Model({
                Model: ColumnModel,
                required: true
            })
        };
    }
}
