
export interface IDBDriver {
    connect(): Promise<void>;
    query<T>(sql: string): Promise<T[]>;
}