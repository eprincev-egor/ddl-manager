import { MigrationModel } from "../../MigrationModel";
import { DDLState } from "../../../state/DDLState";
import { FSDDLState } from "../../../state/FSDDLState";
import { BaseDBObjectModel } from "../../../objects/base-layers/BaseDBObjectModel";
import { IChanges } from "../../../objects/base-layers/BaseDBObjectCollection";

export interface IBaseMigratorParams {
    mode: "dev" | "prod";
    fs: FSDDLState;
    db: DDLState;
}

export abstract class BaseMigrator<
    DBOModel = BaseDBObjectModel<any>
> {
    protected migration: MigrationModel;
    protected mode: IBaseMigratorParams["mode"];
    protected fs: FSDDLState;
    protected db: DDLState;

    constructor(params: IBaseMigratorParams) {
        this.mode = params.mode;
        this.fs = params.fs;
        this.db = params.db;
    }

    migrate(migration: MigrationModel) {
        this.migration = migration;
        
        const {
            created,
            removed,
            changed
        } = this.calcChanges();
        
        removed.forEach((dbo) => {
            this.onRemove(dbo);
        });

        changed.forEach(({prev, next}) => {
            this.onChange(prev, next);
        });

        created.forEach((dbo) => {
            this.onCreate(dbo);
        });
    }

    protected abstract onRemove(dbo: DBOModel): void;
    protected abstract onChange(oldDBO: DBOModel, newDBO: DBOModel): void;
    protected abstract onCreate(dbo: DBOModel): void;
    protected abstract calcChanges(): IChanges<DBOModel>;
}