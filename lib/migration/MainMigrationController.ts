import {DDLState} from "../state/DDLState";
import {MigrationModel} from "./MigrationModel";
import {IMigrationControllerParams, TMigrationMode} from "./IMigrationControllerParams";

import {TriggerController} from "./controllers/TriggerController";
import {ViewsController} from "./controllers/ViewsController";
import {FunctionsController} from "./controllers/FunctionsController";
import {TablesController} from "./controllers/TablesController";

export class MainMigrationController {
    fs: DDLState;
    db: DDLState;
    mode: TMigrationMode;
    private triggersController: TriggerController;
    private viewsController: ViewsController;
    private functionsController: FunctionsController;
    private tablesController: TablesController;

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
        const migration = new MigrationModel();

        this.functionsController.setMigration(migration);
        this.viewsController.setMigration(migration);
        this.tablesController.setMigration(migration);
        this.triggersController.setMigration(migration);

        this.functionsController.generate();
        this.viewsController.generate();
        this.tablesController.generate();
        this.triggersController.generate();

        return migration;
    }
}