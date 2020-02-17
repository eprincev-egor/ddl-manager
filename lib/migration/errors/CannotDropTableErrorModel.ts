import MigrationErrorModel from "./MigrationErrorModel";
import { Types } from "model-layer";

export default class CannotDropTableErrorModel 
extends MigrationErrorModel<CannotDropTableErrorModel> {
    structure() {
        return {
            ...super.structure(),

            tableIdentify: Types.String({
                required: true
            })
        };
    }

    generateMessage(row: this["TInputData"]) {
        return `cannot drop table ${row.tableIdentify}, please use deprecated keyword before table definition`;
    }
}