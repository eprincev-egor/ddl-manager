import BaseController from "./BaseController";

import CommandsCollection from "../commands/CommandsCollection";
import MigrationErrorsCollection from "../errors/MigrationErrorsCollection";

import TriggerCommandModel from "../commands/TriggerCommandModel";

import UnknownTableForTriggerErrorModel from "../errors/UnknownTableForTriggerErrorModel";
import UnknownFunctionForTriggerErrorModel from "../errors/UnknownFunctionForTriggerErrorModel";
import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";


export default class TriggerController extends BaseController {
    generate(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        // drop trigger
        this.db.row.triggers.each((dbTriggerModel) => {
            const dbTriggerIdentify = dbTriggerModel.getIdentify();
            const fsTriggerModel = this.fs.row.triggers.getByIdentify(dbTriggerIdentify);

            if ( fsTriggerModel ) {
                const isEqual = fsTriggerModel.equal(dbTriggerModel);
                if ( !isEqual ) {
                    const dropCommand = new TriggerCommandModel({
                        type: "drop",
                        trigger: dbTriggerModel
                    });
                    commands.push( dropCommand );

                    const createCommand = new TriggerCommandModel({
                        type: "create",
                        trigger: fsTriggerModel
                    });
                    commands.push( createCommand );
                }

                return;
            }

            if ( !dbTriggerModel.get("createdByDDLManager") ) {
                return;
            }

            const command = new TriggerCommandModel({
                type: "drop",
                trigger: dbTriggerModel
            });
            commands.push( command );
        });

        // create trigger
        this.fs.row.triggers.each((fsTriggerModel) => {
            const fsTriggerIdentify = fsTriggerModel.getIdentify();
            const dbTriggerModel = this.db.row.triggers.getByIdentify(fsTriggerIdentify);

            if ( dbTriggerModel ) {
                return;
            }

            const triggerName = fsTriggerModel.get("name");
            if ( triggerName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    objectType: "trigger",
                    name: triggerName
                });

                errors.push(errorModel);
                return;
            }

            const functionIdentify = fsTriggerModel.get("functionIdentify");
            const fsFunctionModel = this.fs.row.functions.getByIdentify(functionIdentify);
            if ( !fsFunctionModel ) {
                const errorModel = new UnknownFunctionForTriggerErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    functionIdentify,
                    triggerName: fsTriggerModel.get("name")
                });

                errors.push(errorModel);
                return;
            }

            const tableIdentify = fsTriggerModel.get("tableIdentify");
            const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

            if ( !fsTableModel ) {
                const errorModel = new UnknownTableForTriggerErrorModel({
                    filePath: fsTriggerModel.get("filePath"),
                    tableIdentify,
                    triggerName: fsTriggerModel.get("name")
                });

                errors.push(errorModel);
                return;
            }

            const command = new TriggerCommandModel({
                type: "create",
                trigger: fsTriggerModel
            });
            commands.push(command);
        });

    }
}