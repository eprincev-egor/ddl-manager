import BaseController from "./BaseController";
import FunctionCommandModel from "../commands/FunctionCommandModel";
import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";
import FunctionModel from "../../objects/FunctionModel";


export default class FunctionsController extends BaseController {

    generate() {
        // drop functions
        this.db.row.functions.each((dbFunctionModel) => {
            const dbFuncIdentify = dbFunctionModel.getIdentify();
            const fsFunctionModel = this.fs.row.functions.getByIdentify(dbFuncIdentify);

            if ( fsFunctionModel ) {
                const hasChanges = !fsFunctionModel.equal(dbFunctionModel);
                if ( hasChanges ) {
                    this.dropFunction( dbFunctionModel );
                    this.createFunction( fsFunctionModel );
                }

                return;
            }

            if ( dbFunctionModel.allowedToDrop() ) {
                this.dropFunction(dbFunctionModel);
            }
        });

        // create functions
        this.forEachNewFunction((fsFunctionModel) => {
            if ( !fsFunctionModel.isValidNameLength() ) {
                this.saveMaxObjectNameSizeError(fsFunctionModel);
                return;
            }

            this.createFunction(fsFunctionModel);
        });
    }

    forEachNewFunction(iteration: ((fsFunctionModel: FunctionModel) => void)) {
        this.fs.row.functions.each((fsFunctionModel) => {
            const funcIdentify = fsFunctionModel.getIdentify();
            const existsDbFunction = !!this.db.row.functions.getByIdentify(funcIdentify);

            if ( existsDbFunction ) {
                return;
            }

            iteration(fsFunctionModel);
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