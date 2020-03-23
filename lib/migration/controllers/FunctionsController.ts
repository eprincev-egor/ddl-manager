import BaseController from "./BaseController";
import FunctionCommandModel from "../commands/FunctionCommandModel";
import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";
import FunctionModel from "../../objects/FunctionModel";

export default class FunctionsController extends BaseController {

    generate() {
        const {
            created,
            removed,
            changed
        } = this.fs.row.functions.compareWithDB(this.db.row.functions);
        
        removed.forEach((functionModel) => {
            if ( functionModel.allowedToDrop() ) {
                this.dropFunction(functionModel);
            }
        });

        changed.forEach(({prev, next}) => {
            this.dropFunction(prev);
            this.createFunction(next);
        });

        created.forEach((functionModel) => {
            if ( !functionModel.isValidNameLength() ) {
                this.saveMaxObjectNameSizeError(functionModel);
                return;
            }

            this.createFunction(functionModel);
        });
    }

    dropFunction(functionModel: FunctionModel) {
        const dropCommand = new FunctionCommandModel({
            type: "drop",
            function: functionModel
        });
        this.migration.addCommand( dropCommand );
    }

    createFunction(functionModel: FunctionModel) {
        const createCommand = new FunctionCommandModel({
            type: "create",
            function: functionModel
        });
        this.migration.addCommand( createCommand );
    }

    saveMaxObjectNameSizeError(functionModel: FunctionModel) {
        const errorModel = new MaxObjectNameSizeErrorModel({
            filePath: functionModel.get("filePath"),
            objectType: "function",
            name: functionModel.get("name")
        });

        this.migration.addError(errorModel);
    }
}