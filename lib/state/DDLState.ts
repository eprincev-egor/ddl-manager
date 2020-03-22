import {Model, Types} from "model-layer";
import FunctionsCollection from "../objects/FunctionsCollection";
import TriggersCollection from "../objects/TriggersCollection";
import ViewsCollection from "../objects/ViewsCollection";
import TablesCollection from "../objects/TablesCollection";
export default class DDLState<Child extends DDLState = DDLState<any>> 
extends Model<Child> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: () => new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: () => new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: () => new ViewsCollection()
            }),
            tables: Types.Collection({
                Collection: TablesCollection,
                default: () => new TablesCollection()
            })
        };
    }
}