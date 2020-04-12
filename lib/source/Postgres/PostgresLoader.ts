import { IDBDriver } from "../../common";
import { PostgresParser } from "./parser/PostgresParser";

import { FunctionsLoader } from "./loaders/FunctionsLoader";
import { TriggersLoader } from "./loaders/TriggersLoader";
import { ViewsLoader } from "./loaders/ViewsLoader";
import { TablesLoader } from "./loaders/TablesLoader";
import { PostgresState } from "./PostgresState";

export class PostgresLoader {
    private driver: IDBDriver;
    private parser: PostgresParser;

    private functionsLoader: FunctionsLoader;
    private triggersLoader: TriggersLoader;
    private viewsLoader: ViewsLoader;
    private tablesLoader: TablesLoader;

    constructor(driver: IDBDriver) {
        this.driver = driver;
        this.parser = new PostgresParser();

        const loaderParams = {
            driver,
            parser: this.parser
        };
        this.functionsLoader = new FunctionsLoader(loaderParams);
        this.triggersLoader = new TriggersLoader(loaderParams);
        this.viewsLoader = new ViewsLoader(loaderParams);
        this.tablesLoader = new TablesLoader(driver);
    }

    async load(state: PostgresState) {
        await this.driver.connect();

        const functions = await this.functionsLoader.load();
        const triggers = await this.triggersLoader.load();
        const views = await this.viewsLoader.load();
        const tables = await this.tablesLoader.load();

        state.addFunctions(functions);
        state.addTriggers(triggers);
        state.addViews(views);
        state.addTables(tables);
    }
}