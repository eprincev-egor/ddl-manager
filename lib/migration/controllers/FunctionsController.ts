import BaseController from "./BaseController";

import CommandsCollection from "../commands/CommandsCollection";
import MigrationErrorsCollection from "../errors/MigrationErrorsCollection";

import FunctionCommandModel from "../commands/FunctionCommandModel";

import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";


export default class FunctionsController extends BaseController {
    generate(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        // drop functions
        this.db.row.functions.each((dbFunctionModel) => {
            const dbFuncIdentify = dbFunctionModel.getIdentify();
            const fsFunctionModel = this.fs.row.functions.getByIdentify(dbFuncIdentify);

            if ( fsFunctionModel ) {
                const isEqual = fsFunctionModel.equal(dbFunctionModel);
                if ( !isEqual ) {
                    
                    const dropCommand = new FunctionCommandModel({
                        type: "drop",
                        function: dbFunctionModel
                    });
                    commands.push( dropCommand );

                    const createCommand = new FunctionCommandModel({
                        type: "create",
                        function: fsFunctionModel
                    });
                    commands.push( createCommand );
                }

                return;
            }

            if ( !dbFunctionModel.get("createdByDDLManager") ) {
                return;
            }

            const command = new FunctionCommandModel({
                type: "drop",
                function: dbFunctionModel
            });
            commands.push( command );
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

                errors.push(errorModel);
                return;
            }

            const command = new FunctionCommandModel({
                type: "create",
                function: fsFunctionModel
            });
            commands.push( command );
        });
    }
}