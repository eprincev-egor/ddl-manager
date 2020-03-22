import BaseController from "./BaseController";

import CommandsCollection from "../commands/CommandsCollection";
import MigrationErrorsCollection from "../errors/MigrationErrorsCollection";

import ColumnCommandModel from "../commands/ColumnCommandModel";

import UnknownTableForExtensionErrorModel from "../errors/UnknownTableForExtensionErrorModel";


export default class ExtensionsController extends BaseController {
    generate(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        this.fs.row.extensions.each((fsExtensionModel) => {
            const tableIdentify = fsExtensionModel.get("forTableIdentify");
            const fsTableModel = this.fs.row.tables.getByIdentify(tableIdentify);

            if ( !fsTableModel ) {
                const errorModel = new UnknownTableForExtensionErrorModel({
                    filePath: fsExtensionModel.get("filePath"),
                    tableIdentify,
                    extensionName: fsExtensionModel.get("name")
                });

                errors.push(errorModel);
                return;
            }

            const fsExtensionColumns = fsExtensionModel.get("columns");
            for (const fsColumnModel of fsExtensionColumns) {
                const createColumnCommand = new ColumnCommandModel({
                    type: "create",
                    tableIdentify,
                    column: fsColumnModel
                });
                commands.push(createColumnCommand);
            }
        });

    }
}