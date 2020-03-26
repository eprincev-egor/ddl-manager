import {DBOWithoutDataStrategyController} from "./base-layers/DBOWithoutDataStrategyController";
import {TriggerModel} from "../../objects/TriggerModel";

import {TriggerCommandModel} from "../commands/TriggerCommandModel";

import {UnknownTableForTriggerErrorModel} from "../errors/UnknownTableForTriggerErrorModel";
import {UnknownFunctionForTriggerErrorModel} from "../errors/UnknownFunctionForTriggerErrorModel";


export 
class TriggerController 
extends DBOWithoutDataStrategyController<TriggerModel> {
    
    protected detectChanges() {
        const dbTriggers = this.db.row.triggers;
        const fsTriggers = this.fs.row.triggers;
        return fsTriggers.compareWithDB(dbTriggers);
    }

    protected validate(triggerModel: TriggerModel) {
        this.validateNameLength(triggerModel);
        this.validateExistsFunction(triggerModel);
        this.validateExistsTable(triggerModel);
    }

    private validateExistsFunction(triggerModel: TriggerModel) {
        const functionIdentify = triggerModel.get("functionIdentify");
        const fsFunctionModel = this.fs.row.functions.getByIdentify(functionIdentify);

        if ( !fsFunctionModel ) {
            const error = new UnknownFunctionForTriggerErrorModel({
                filePath: triggerModel.get("filePath"),
                functionIdentify,
                triggerName: triggerModel.get("name")
            });
            this.throwErrorModel(error);
        }
    }

    private validateExistsTable(triggerModel: TriggerModel) {
        const tableIdentify = triggerModel.get("tableIdentify");
        const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

        if ( !fsTableModel ) {
            const error = new UnknownTableForTriggerErrorModel({
                filePath: triggerModel.get("filePath"),
                tableIdentify,
                triggerName: triggerModel.get("name")
            });
            this.throwErrorModel(error);
        }
    }

    protected getDropCommand(triggerModel: TriggerModel) {
        return new TriggerCommandModel({
            type: "drop",
            trigger: triggerModel
        });
    }

    protected getCreateCommand(triggerModel: TriggerModel) {
        return new TriggerCommandModel({
            type: "create",
            trigger: triggerModel
        });
    }
}