import { BaseValidator } from "./BaseValidator";

import {UnknownTableForTriggerErrorModel} from "../../errors/UnknownTableForTriggerErrorModel";
import {UnknownFunctionForTriggerErrorModel} from "../../errors/UnknownFunctionForTriggerErrorModel";
import { TriggerModel } from "../../../objects/TriggerModel";


export class TriggerValidator extends BaseValidator {
    validate(dbo: TriggerModel) {
        const errorModel = (
            this.validateExistsFunction(dbo) ||
            this.validateExistsTable(dbo)
        );
        return errorModel;
    }

    private validateExistsFunction(triggerModel: TriggerModel) {
        const functionIdentify = triggerModel.get("functionIdentify");
        const fsFunctionModel = this.fs.row.functions.getByIdentify(functionIdentify);

        if ( !fsFunctionModel ) {
            const errorModel = new UnknownFunctionForTriggerErrorModel({
                filePath: triggerModel.get("filePath"),
                functionIdentify,
                triggerName: triggerModel.get("name")
            });

            return errorModel;
        }
    }

    private validateExistsTable(triggerModel: TriggerModel) {
        const tableIdentify = triggerModel.get("tableIdentify");
        const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

        if ( !fsTableModel ) {
            const errorModel = new UnknownTableForTriggerErrorModel({
                filePath: triggerModel.get("filePath"),
                tableIdentify,
                triggerName: triggerModel.get("name")
            });
            
            return errorModel;
        }
    }

}