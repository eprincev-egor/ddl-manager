import { IDBO, IDBOSource, IDBODestination } from "../IDBO";
import { Directory } from "./Directory";

export class FileSystem
implements 
    IDBOSource,
    IDBODestination
{
    private root: Directory;

    async load() {
        return;
    }

    async create(dbo: IDBO): Promise<void> {
        return;
    }
    
    async drop(dbo: IDBO): Promise<void> {
        return;
    }
}