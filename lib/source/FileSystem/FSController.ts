import { FSReader } from "./FSReader";
import { FSDriver } from "./FSDriver";
import { Directory } from "./Directory";
import { File } from "./File";

interface IFSControllerParams {
    driver: FSDriver;
    reader: FSReader;
}

export class FSController {
    private driver: FSDriver;
    private reader: FSReader;
    private root: Directory;

    constructor(params: IFSControllerParams) {
        this.driver = params.driver;
        this.reader = params.reader;

        this.listenDriverEvents();
    }

    listenDriverEvents() {
        this.driver.on("change", (path: string) => {
            this.onFSChangeOrRenameOrAdd(path);
        });

        this.driver.on("unlink", (path: string) => {
            this.onFSUnlink(path);
        });
    }

    async load(directoryPath: string): Promise<void> {
        const directory = await this.reader.readDirectory(directoryPath);
        this.root = directory;
    }
    
    private async onFSChangeOrRenameOrAdd(filePath: string) {
        const file = await this.reader.readFile(filePath);

        const existentFile = this.root.getFileByPath(filePath);
        const fileWithSameContent = this.root.getFileBySQL(file.sql);

        let isRename = false;
        if ( fileWithSameContent ) {
            const oldFilePath = fileWithSameContent.path;
            const existsOldFile = await this.driver.existsFile(oldFilePath);

            if ( !existsOldFile ) {
                isRename = true;
            }
        }

        const isChange = (
            !isRename &&
            !!existentFile
        );

        if ( isRename ) {
            this.onFSRename(fileWithSameContent, file);
        }
        else if ( isChange ) {
            this.onFSChange(file);
        }
        else {
            this.onFSAdd(file);
        }
    }

    private onFSChange(file: File) {
        this.root.deepRemoveFile( file.path );
        this.root.deepAddFile( file );
    }

    private onFSAdd(file: File) {
        this.root.deepAddFile( file );
    }

    private onFSRename(oldFile: File, newFile: File) {
        this.root.deepRemoveFile( oldFile.path );
        this.root.deepAddFile( newFile );
    }

    private async onFSUnlink(filePath: string) {
        this.root.deepRemoveFile( filePath );
    }
}
