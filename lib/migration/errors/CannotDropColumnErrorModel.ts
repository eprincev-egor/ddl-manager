import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class CannotDropColumnErrorModel 
extends MigrationErrorModel<CannotDropColumnErrorModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            }),
            columnKey: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `cannot drop column ${row.tableIdentify}.${row.columnKey}, please use deprecated section`;
    }
}