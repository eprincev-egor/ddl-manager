import {DDLState} from "../../../state/DDLState";
import {FSDDLState} from "../../../state/FSDDLState";
import {
    IMigrationControllerParams, 
    TMigrationMode
} from "../../IMigrationControllerParams";
import {MigrationModel} from "../../MigrationModel";

export 
abstract class BaseController 
implements IMigrationControllerParams {
    fs: FSDDLState; 
    db: DDLState;
    mode: TMigrationMode;
    protected migration: MigrationModel;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";
    }

    setMigration(migration: MigrationModel) {
        this.migration = migration
    }

}