import DDLState from "../../state/DDLState";
import {
    IMigrationControllerParams, 
    TMigrationMode
} from "../IMigrationControllerParams";

export default abstract class BaseController implements IMigrationControllerParams {
    fs: DDLState; 
    db: DDLState;
    mode: TMigrationMode;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";
    }
}