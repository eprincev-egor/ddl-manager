import {Model, Types} from "model-layer";
import {FunctionsCollection} from "./Functions";
import {TriggersCollection} from "./Triggers";
import {ViewsCollection} from "./Views";
import Migration from "./migration/Migration";

export default class State extends Model<State> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: new ViewsCollection()
            })
        };
    }

    generateMigration(dbState: State): Migration {
        const fsState: State = this;
        return new Migration();
    }
}