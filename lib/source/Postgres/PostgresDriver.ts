import pg from "pg";
import { IDBDriver } from "../../common";

export interface IPostgresDriverParams {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
}

export class PostgresDriver
implements IDBDriver {
    private db: pg.Client;

    constructor(params: IPostgresDriverParams) {
        this.db = new pg.Client(params);
    }

    async connect() {
        await this.db.connect();
    }

    async query<T>(sql: string): Promise<T[]> {
        const result = this.db.query<T>(sql);
        return (await result).rows;
    }
}