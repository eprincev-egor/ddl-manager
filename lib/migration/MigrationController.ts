import State from "../state/State";
import MigrationModel from "./MigrationModel";
import {IMigrationControllerParams, TMigrationMode} from "./IMigrationControllerParams";

import CommandsCollection from "./commands/CommandsCollection";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";

import MigrationTriggerController from "./controllers/MigrationTriggerController";
import MigrationViewsController from "./controllers/MigrationViewsController";
import MigrationFunctionsController from "./controllers/MigrationFunctionsController";
import MigrationTablesController from "./controllers/MigrationTablesController";

export default class MigrationController {
    fs: State;
    db: State;
    mode: TMigrationMode;
    triggersController: MigrationTriggerController;
    viewsController: MigrationViewsController;
    functionsController: MigrationFunctionsController;
    tablesController: MigrationTablesController;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";

        this.triggersController = new MigrationTriggerController(params);
        this.viewsController = new MigrationViewsController(params);
        this.functionsController = new MigrationFunctionsController(params);
        this.tablesController = new MigrationTablesController(params);
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