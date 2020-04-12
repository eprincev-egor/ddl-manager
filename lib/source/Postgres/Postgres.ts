import { AbstractDatabase } from "../AbstractDatabase";
import { IDBDriver } from "../../common";
import { PostgresLoader } from "./PostgresLoader";
import { PostgresState } from "./PostgresState";

export class Postgres 
extends AbstractDatabase<PostgresState> {
    private loader: PostgresLoader;

    constructor(driver: IDBDriver) {
        super(driver);
        
        this.loader = new PostgresLoader(driver);
    }

    createState() {
        const state = new PostgresState();
        return state;
    }

    async load() {
        await this.driver.connect();
        await this.loader.load(this.state);
    }
}