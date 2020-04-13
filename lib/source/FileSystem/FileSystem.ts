import { IDBO, IDBOSource, IDBODestination, IDBOParser } from "../../common";
import { FSDriver } from "./FSDriver";
import { FSReader } from "./FSReader";
import { FSController } from "./FSController";

export interface IFileSystemParams<TDestination extends IDBODestination> {
    state: TDestination["state"];
    parser: IDBOParser;
    rootPath: string;
}

export class FileSystem<TDestination extends IDBODestination>
implements 
    IDBOSource,
    IDBODestination
{
    state: TDestination["state"];
    private driver: FSDriver;
    private reader: FSReader;
    private parser: IDBOParser;
    private controller: FSController;
    private rootPath: string;

    constructor(params: IFileSystemParams<TDestination>) {
        this.state = params.state;
        this.parser = params.parser;
        this.rootPath = params.rootPath;

        this.driver = new FSDriver();
        this.reader = new FSReader({
            driver: this.driver,
            parser: this.parser
        });
        this.controller = new FSController({
            driver: this.driver,
            reader: this.reader
        });
    }

    async load() {
        await this.controller.load(this.rootPath);
    }

    async create(dbo: IDBO): Promise<void> {
        return;
    }
    
    async drop(dbo: IDBO): Promise<void> {
        return;
    }
}