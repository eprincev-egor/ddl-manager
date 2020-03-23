import {Collection} from "model-layer";
import BaseDBObjectModel from "./BaseDBObjectModel";

interface IChildCollection {
    Model(): (new (...args: any[]) => BaseDBObjectModel<any>);
}

export interface IChanges<TModel> {
    removed: TModel[];
    created: TModel[];
    changed: {
        next: TModel,
        prev: TModel
    }[];
}

export default abstract class BaseDBObjectCollection<
    ChildCollection extends BaseDBObjectCollection<any> & IChildCollection
> extends Collection<ChildCollection> {
    
    getByIdentify(identify: string): this["TModel"] {
        const existsModel = this.find((model) =>
            model.getIdentify() === identify
        );

        return existsModel;
    }

    compareWithDB(otherCollection: this): IChanges<this["TModel"]> {
        const changes: IChanges<this["TModel"]> = {
            removed: [],
            created: [],
            changed: []
        };

        otherCollection.each((otherDBO) => {
            const identify = otherDBO.getIdentify();
            const thisDBO = this.getByIdentify(identify);

            if ( thisDBO ) {
                const hasChanges = !thisDBO.equal(otherDBO);
                if ( hasChanges ) {
                    changes.changed.push({
                        prev: otherDBO,
                        next: thisDBO
                    });
                }

                return;
            }

            changes.removed.push(otherDBO);
        });

        this.each((thisDBO) => {
            const identify = thisDBO.getIdentify();
            const existsDBOinOtherCollection = !!otherCollection.getByIdentify(identify);

            if ( !existsDBOinOtherCollection ) {
                changes.created.push(thisDBO);
            }
        });

        return changes;
    }
}