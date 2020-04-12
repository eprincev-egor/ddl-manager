import { AbstractDatabase } from "../AbstractDatabase";
import { IDBDriver } from "../../common";
import { PostgresLoader } from "./PostgresLoader";

export class Postgres 
extends AbstractDatabase {
    private loader: PostgresLoader;

    constructor(driver: IDBDriver) {
        super(driver);
        
        this.loader = new PostgresLoader(driver);
    }

    async load() {
        await this.driver.connect();
        await this.loader.load(this.state);
    }
}