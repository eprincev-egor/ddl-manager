import { ITableDBO, IDBO } from "./common";

export abstract class AbstractDDLState<DBOTypes extends {
    table: ITableDBO;
    view: IDBO;
    trigger: IDBO;
    function: IDBO;
}> {
    private tables: DBOTypes["table"][];
    private views: DBOTypes["view"][];
    private triggers: DBOTypes["trigger"][];
    private functions: DBOTypes["function"][];

    constructor() {
        this.tables = [];
        this.views = [];
        this.triggers = [];
        this.functions = [];
    }

    addViews(views: DBOTypes["view"][]) {
        this.views.push( ...views );
    }

    addTables(tables: DBOTypes["table"][]) {
        this.tables.push( ...tables );
    }

    addFunctions(functions: DBOTypes["function"][]) {
        this.functions.push( ...functions );
    }

    addTriggers(triggers: DBOTypes["trigger"][]) {
        this.triggers.push( ...triggers );
    }
}