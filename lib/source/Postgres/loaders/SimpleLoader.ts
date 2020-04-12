import { IDBDriver, IDBOParser, IDBO } from "../../../common";
import { ILoader } from "./ILoader";

export interface ILoaderParams {
    driver: IDBDriver;
    parser: IDBOParser;
}

interface TRow {
    ddl: string;
}

export abstract class AbstractLoader<TDBO extends IDBO>
implements ILoader {
    protected driver: IDBDriver;
    protected parser: IDBOParser;

    constructor(params: ILoaderParams) {
        this.driver = params.driver;
        this.parser = params.parser;
    }
    
    async load(): Promise<TDBO[]> {
        const sql = this.getSelectObjectsSQL();
        const rows = await this.driver.query<TRow>(sql);
        const objects = rows.map(row => 
            this.parseDBO(row)
        );
        return objects;
    }

    protected parseDBO(row: TRow): TDBO {
        const ddl = row.ddl;
        const objects = this.parser.parse(ddl) as any[];
        const dbo = objects[0] as TDBO;
        return dbo;
    }

    protected abstract getSelectObjectsSQL(): string;
}