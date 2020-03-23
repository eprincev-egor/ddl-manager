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
                const isEqual = fsFunctionModel.equal(dbFunctionModel);
                if ( !isEqual ) {
                    this.dropFunction( dbFunctionModel );
                    this.createFunction( fsFunctionModel );
                }

                return;
            }

            if ( !dbFunctionModel.get("createdByDDLManager") ) {
                return;
            }

            this.dropFunction(dbFunctionModel);
        });

        // create functions
        this.fs.row.functions.each((fsFunctionModel) => {
            const fsFuncIdentify = fsFunctionModel.getIdentify();
            const dbFunctionModel = this.db.row.functions.getByIdentify(fsFuncIdentify);

            if ( dbFunctionModel ) {
                return;
            }

            const functionName = fsFunctionModel.get("name");
            if ( functionName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsFunctionModel.get("filePath"),
                    objectType: "function",
                    name: functionName
                });

                this.migration.addError(errorModel);
                return;
            }

            this.createFunction(fsFunctionModel);
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
}