import { BaseDBObjectModel } from "../objects/base-layers/BaseDBObjectModel";

export interface IChanges<TModel> {
    removed: TModel[];
    created: TModel[];
    changed: {
        next: TModel,
        prev: TModel
    }[];
}

export class Changes<TModel extends BaseDBObjectModel<any>>
implements IChanges<TModel> {
    removed: TModel[];
    created: TModel[];
    changed: {
        next: TModel,
        prev: TModel
    }[];

    constructor() {
        this.removed = [];
        this.created = [];
        this.changed = [];
    }

    detect(fsModels: TModel[], dbModels: TModel[]) {
        this.detectCratedAndChanged(fsModels, dbModels);
        this.detectRemoved(fsModels, dbModels);
    }

    private detectCratedAndChanged(fsModels: TModel[], dbModels: TModel[]) {
        dbModels.forEach((dbModel) => {
            const identify = dbModel.getIdentify();
            const fsModel = fsModels.find((model) =>
                model.getIdentify() === identify
            );

            if ( fsModel ) {
                const hasChanges = !fsModel.equal(dbModel);
                if ( hasChanges ) {
                    this.changed.push({
                        prev: dbModel,
                        next: fsModel
                    });
                }

                return;
            }

            this.removed.push(dbModel);
        });
    }

    private detectRemoved(fsModels: TModel[], dbModels: TModel[]) {
        fsModels.forEach((fsModel) => {
            const identify = fsModel.getIdentify();
            const existsDBOinOtherCollection = !!dbModels.find((model) =>
                model.getIdentify() === identify
            );

            if ( !existsDBOinOtherCollection ) {
                this.created.push(fsModel);
            }
        });
    }
}