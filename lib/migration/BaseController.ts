import State from "../state/State";
import {
    IMigrationControllerParams, 
    TMigrationMode
} from "./IMigrationControllerParams";

import CommandsCollection from "./commands/CommandsCollection";
import MigrationErrorsCollection from "./errors/MigrationErrorsCollection";

export default abstract class BaseController implements IMigrationControllerParams {
    fs: State; 
    db: State;
    mode: TMigrationMode;

    constructor(params: IMigrationControllerParams) {
        this.fs = params.fs;
        this.db = params.db;
        this.mode = params.mode || "prod";
    }
}