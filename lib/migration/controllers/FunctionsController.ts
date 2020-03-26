import {DBOWithoutDataStrategyController} from "./base-layers/DBOWithoutDataStrategyController";
import {FunctionCommandModel} from "../commands/FunctionCommandModel";
import {FunctionModel} from "../../objects/FunctionModel";

export 
class FunctionsController 
extends DBOWithoutDataStrategyController<FunctionModel> {

    protected detectChanges() {
        const dbFunctions = this.db.row.functions;
        const fsFunctions = this.fs.row.functions;
        return fsFunctions.compareWithDB(dbFunctions);
    }

    protected validate(functionModel: FunctionModel) {
        this.validateNameLength(functionModel);
    }

    protected getDropCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "drop",
            function: functionModel
        });
    }

    protected getCreateCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "create",
            function: functionModel
        });
    }

}