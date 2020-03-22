import State from "../state/State";
import MigrationModel from "./MigrationModel";
import {IMigrationControllerParams, TMigrationMode} from "./IMigrationControllerParams";

import CommandsCollection from "./commands/CommandsCollection";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";

import TriggerController from "./controllers/TriggerController";
import ViewsController from "./controllers/ViewsController";
import FunctionsController from "./controllers/FunctionsController";
import TablesController from "./controllers/TablesController";

export default class MigrationController {
    fs: State;
    db: State;
    mode: TMigrationMode;
    triggersController: TriggerController;
    viewsController: ViewsController;
    functionsController: FunctionsController;
    tablesController: TablesController;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";

        this.triggersController = new TriggerController(params);
        this.viewsController = new ViewsController(params);
        this.functionsController = new FunctionsController(params);
        this.tablesController = new TablesController(params);
    }

    generateMigration(): MigrationModel {
        const commands: CommandsCollection["TInput"] = [];
        const errors: MigrationErrorsCollection["TModel"][] = [];

        this.functionsController.generate(
            commands,
            errors
        );

        this.viewsController.generate(
            commands,
            errors
        );

        this.tablesController.generate(
            commands,
            errors
        );

        this.triggersController.generate(
            commands,
            errors
        );

        // output migration
        return new MigrationModel({
            commands,
            errors
        });
    }
}