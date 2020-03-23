import DefaultStrategyController from "./base-layers/DefaultStrategyController";
import FunctionCommandModel from "../commands/FunctionCommandModel";
import FunctionModel from "../../objects/FunctionModel";

export default 
class FunctionsController 
extends DefaultStrategyController {

    detectChanges() {
        const dbFunctions = this.db.row.functions;
        const fsFunctions = this.fs.row.functions;
        return fsFunctions.compareWithDB(dbFunctions);
    }

    validate(functionModel: FunctionModel) {
        return [
            this.validateNameLength(functionModel)
        ];
    }

    getDropCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "drop",
            function: functionModel
        });
    }

    getCreateCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "create",
            function: functionModel
        });
    }

}