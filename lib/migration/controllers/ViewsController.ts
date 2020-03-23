import BaseController from "./base-layers/BaseController";
import ViewCommandModel from "../commands/ViewCommandModel";
import MaxObjectNameSizeErrorModel from "../errors/MaxObjectNameSizeErrorModel";


export default class ViewsController extends BaseController {
    generate() {

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
                    this.migration.addCommand( dropCommand );

                    const createCommand = new ViewCommandModel({
                        type: "create",
                        view: fsViewModel
                    });
                    this.migration.addCommand( createCommand );
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
            this.migration.addCommand( command );
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

                this.migration.addError(errorModel);
                return;
            }

            const command = new ViewCommandModel({
                type: "create",
                view: fsViewModel
            });
            this.migration.addCommand(command);
        });

    }
}