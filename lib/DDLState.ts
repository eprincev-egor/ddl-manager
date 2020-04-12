import { ITableDBO, IDBO } from "./common";

export class DDLState {
    private tables: ITableDBO[];
    private views: IDBO[];
    private triggers: IDBO[];
    private functions: IDBO[];

    constructor() {
        this.tables = [];
        this.views = [];
        this.triggers = [];
        this.functions = [];
    }

    addViews(views: IDBO[]) {
        this.views.push( ...views );
    }

    addTables(tables: ITableDBO[]) {
        this.tables.push( ...tables );
    }

    addFunctions(functions: IDBO[]) {
        this.functions.push( ...functions );
    }

    addTriggers(triggers: IDBO[]) {
        this.triggers.push( ...triggers );
    }
}