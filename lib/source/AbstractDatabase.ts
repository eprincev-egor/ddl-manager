import { IDBO, IDBOSource, IDBODestination, IDBDriver } from "../common";
import { AbstractDDLState } from "../DDLState";

export abstract class AbstractDatabase<TDDLState extends AbstractDDLState<any>>
implements 
    IDBOSource, 
    IDBODestination
{
    state: TDDLState;
    protected driver: IDBDriver;

    constructor(driver: IDBDriver) {
        this.driver = driver;
        this.state = this.createState();
    }

    abstract createState(): TDDLState;
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