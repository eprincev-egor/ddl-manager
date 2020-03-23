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

    compareWithDB(dbCollection: this): IChanges<this["TModel"]> {
        const changes: IChanges<this["TModel"]> = {
            removed: [],
            created: [],
            changed: []
        };

        dbCollection.each((dbObject) => {
            const identify = dbObject.getIdentify();
            const fsObject = this.getByIdentify(identify);

            if ( fsObject ) {
                const hasChanges = !fsObject.equal(dbObject);
                if ( hasChanges ) {
                    changes.changed.push({
                        prev: dbObject,
                        next: fsObject
                    });
                }

                return;
            }

            changes.removed.push(dbObject);
        });

        this.each((fsObject) => {
            const identify = fsObject.getIdentify();
            const existsDBOinOtherCollection = !!dbCollection.getByIdentify(identify);

            if ( !existsDBOinOtherCollection ) {
                changes.created.push(fsObject);
            }
        });

        return changes;
    }
}