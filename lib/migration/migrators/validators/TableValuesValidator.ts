import { BaseValidator } from "./BaseValidator";
import { TableModel } from "../../../objects/TableModel";
import { ExpectedPrimaryKeyForRowsErrorModel } from "../../errors/ExpectedPrimaryKeyForRowsErrorModel";

export class TableValuesValidator extends BaseValidator {
    
    validate(tableModel: TableModel): ExpectedPrimaryKeyForRowsErrorModel {

        const hasValues = !!tableModel.get("values");
        const hasPrimaryKey = !!tableModel.get("primaryKey");

        if ( hasValues && !hasPrimaryKey ) {
            const errorModel = new ExpectedPrimaryKeyForRowsErrorModel({
                filePath: tableModel.get("filePath"),
                tableIdentify: tableModel.getIdentify()
            });

            return errorModel;
        }
    }
}