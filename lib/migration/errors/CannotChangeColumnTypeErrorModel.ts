import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class CannotChangeColumnTypeErrorModel
extends MigrationErrorModel<CannotChangeColumnTypeErrorModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            columnKey: Types.String({
                required: true
            }),
            oldType: Types.String({
                required: true
            }),
            newType: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `cannot change column type ${row.tableIdentify}.${row.columnKey} from ${row.oldType} to ${row.newType}`;
    }
}