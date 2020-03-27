import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";
import { DDLState } from "../../../state/DDLState";
import { FSDDLState } from "../../../state/FSDDLState";
import { InputError } from "../../MigrationModel";
import { IBaseMigratorParams } from "../base-layers/BaseMigrator";

export 
abstract class BaseValidator {
    protected mode: IBaseMigratorParams["mode"];
    protected fs: FSDDLState;
    protected db: DDLState;

    constructor(params: IBaseMigratorParams) {
        this.mode = params.mode;
        this.fs = params.fs;
        this.db = params.db;
    }

    abstract validate(dbo: BaseDBObjectModel<any>): InputError;
}