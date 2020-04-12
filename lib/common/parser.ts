import { IDBO, IExtension } from "./objects";


export interface IDBOParser {
    parse(sql: string): (IDBO | IExtension)[];
}
