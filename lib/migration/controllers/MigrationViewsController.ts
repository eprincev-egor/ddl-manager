import BaseController from "./BaseController";

import CommandsCollection from "../commands/CommandsCollection";
import MigrationErrorsCollection from "../errors/MigrationErrorsCollection";

import ViewCommandModel from "../commands/ViewCommandModel";

import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";


export default class MigrationViewsController extends BaseController {
    generate(
        commands: CommandsCollection["TInput"],
        errors: MigrationErrorsCollection["TModel"][]
    ) {

        // drop views
        this.db.row.views.each((dbViewModel) => {
            const dbViewIdentify = dbViewModel.getIdentify();
            const fsViewModel = this.fs.row.views.getByIdentify(dbViewIdentify);

            if ( fsViewModel ) {
                const isEqual = fsViewModel.equal(dbViewModel);
                if ( !isEqual ) {
                    const dropCommand = new ViewCommandModel({
                        type: "drop",
                        view: dbViewModel
                    });
                    commands.push( dropCommand );

                    const createCommand = new ViewCommandModel({
                        type: "create",
                        view: fsViewModel
                    });
                    commands.push( createCommand );
                }

                return;
            }

            if ( !dbViewModel.get("createdByDDLManager") ) {
                return;
            }

            const command = new ViewCommandModel({
                type: "drop",
                view: dbViewModel
            });
            commands.push( command );
        });

        // create views
        this.fs.row.views.each((fsViewModel) => {
            const fsViewIdentify = fsViewModel.getIdentify();
            const dbViewModel = this.db.row.views.getByIdentify(fsViewIdentify);

            if ( dbViewModel ) {
                return;
            }

            const viewName = fsViewModel.get("name");
            if ( viewName.length > 64 ) {
                const errorModel = new MaxObjectNameSizeErrorModel({
                    filePath: fsViewModel.get("filePath"),
                    objectType: "view",
                    name: viewName
                });

                errors.push(errorModel);
                return;
            }

            const command = new ViewCommandModel({
                type: "create",
                view: fsViewModel
            });
            commands.push(command);
        });

    }
}