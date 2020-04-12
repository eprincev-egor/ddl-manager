import { IDBO, IDBOSource, IDBODestination, IDBDriver } from "../common";
import { DDLState } from "../state/DDLState";

export abstract class AbstractDatabase
implements 
    IDBOSource, 
    IDBODestination
{
    state: DDLState;
    protected driver: IDBDriver;

    constructor(driver: IDBDriver) {
        this.state = new DDLState();
        this.driver = driver;
    }

    abstract load(): Promise<void>;

    async drop(dbo: IDBO): Promise<void> {
        const sql = dbo.toDropSQL();
        await this.driver.query(sql);
    }

    async create(dbo: IDBO): Promise<void> {
        const sql = dbo.toCreateSQL();
        await this.driver.query(sql);
    }
}