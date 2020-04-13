import { ITableDBO, IDBO } from "./common";
import { compareMapWithDestination } from "./utils/compare";

interface IByIdentify<T> {
    [identify: string]: T;
}

export abstract class AbstractDDLState<DBOTypes extends {
    table: ITableDBO;
    view: IDBO;
    trigger: IDBO;
    function: IDBO;
}> {
    private tables: IByIdentify<DBOTypes["table"]> = {};
    private views: IByIdentify<DBOTypes["view"]> = {};
    private triggers: IByIdentify<DBOTypes["trigger"]> = {};
    private functions: IByIdentify<DBOTypes["function"]> = {};

    addViews(views: DBOTypes["view"][]) {
        this.addObjectsTo(views, this.views);
    }

    addTables(tables: DBOTypes["table"][]) {
        this.addObjectsTo(tables, this.tables);
    }

    addFunctions(functions: DBOTypes["function"][]) {
        this.addObjectsTo(functions, this.functions);
    }

    addTriggers(triggers: DBOTypes["trigger"][]) {
        this.addObjectsTo(triggers, this.triggers);
    }

    private addObjectsTo<TObject extends IDBO>(objects: TObject[], map: IByIdentify<TObject>) {
        for (const obj of objects) {
            const identify = obj.getIdentify();
            map[ identify ] = obj;
        }
    }

    compareWithDestination(destinationState: this) {
        const sourceState = this;

        const totalChanges = {
            tables: compareMapWithDestination(
                sourceState.tables,
                destinationState.tables
            ),
            views: compareMapWithDestination(
                sourceState.views,
                destinationState.views
            ),
            functions: compareMapWithDestination(
                sourceState.functions,
                destinationState.functions
            ),
            triggers: compareMapWithDestination(
                sourceState.triggers,
                destinationState.triggers
            )
        };

        return totalChanges;
    }
}