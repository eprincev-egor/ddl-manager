import {SimpleMigrator} from "./base-layers/SimpleMigrator";
import {FunctionCommandModel} from "../commands/FunctionCommandModel";
import {FunctionModel} from "../../objects/FunctionModel";
import { NameValidator } from "./validators/NameValidator";

export class FunctionsMigrator
extends SimpleMigrator<FunctionModel> {

    protected calcChanges() {
        const fsFunctions = this.fs.row.functions;
        const dbFunctions = this.db.row.functions;
        const changes = fsFunctions.compareWithDB(dbFunctions);
        return changes;
    }

    protected getValidators() {
        return [
            NameValidator
        ];
    }

    protected createDropCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "drop",
            function: functionModel
        });
    }

    protected createCreateCommand(functionModel: FunctionModel) {
        return new FunctionCommandModel({
            type: "create",
            function: functionModel
        });
    }

}