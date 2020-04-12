import { AbstractDatabase } from "../AbstractDatabase";
import { IDBDriver } from "../../common";
import { FunctionsLoader } from "./loaders/FunctionsLoader";
import { PostgresParser } from "./parser/PostgresParser";

export class Postgres extends AbstractDatabase {
    private functionsLoader: FunctionsLoader;

    constructor(driver: IDBDriver) {
        super(driver);
        
        const parser = new PostgresParser();
        this.functionsLoader = new FunctionsLoader({
            driver,
            parser
        });
    }

    async load() {
        await this.driver.connect();

        const functions = await this.functionsLoader.load();
    }
}