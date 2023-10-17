import fs from "fs";
import glob from "glob";
import { EventEmitter } from "events";
import { FileParser } from "../parser";
import { File } from "./File";
import { FilesState } from "./FilesState";
import { formatPath, getSubPath, prepareError } from "./utils";

export class FileReader extends EventEmitter {

    static read(rootFolders: string[], onError?: (err: Error) => void) {
        const reader = new FileReader(rootFolders);

        if ( onError ) {
            reader.on("error", onError);
        }

        reader.read();
        return reader.state;
    }

    readonly state: FilesState;
    
    private rootFolders: string[];
    private fileParser: FileParser;
    
    constructor(rootFolders: string[]) {
        super();

        this.fileParser = new FileParser();
        this.state = new FilesState();

        this.rootFolders = rootFolders;
    }

    parseFile(rootFolderPath: string, filePath: string): File | undefined {
        const sqlFile = this.fileParser.parseFile(filePath);
        const subPath = getSubPath(rootFolderPath, filePath);

        const fileName = filePath.split(/[/\\]/).pop() as string;

        return new File({
            name: fileName,
            folder: rootFolderPath,
            path: formatPath(subPath),
            content: sqlFile
        });
    }

    read() {
        for (const folderPath of this.rootFolders) {
            this.readFolder(folderPath);
        }
    }

    private readFolder(folderPath: string) {

        if ( !fs.existsSync(folderPath) ) {
            throw new Error(`folder "${ folderPath }" not found`);
        }

        // fill this.files, make array of object:
        // {
        //   name: "some-file-name.sql",
        //   path: "/path/to/some-file-name.sql",
        //   content: {
        //        functions: [],
        //        triggers: []
        //   }
        // }

        const files = glob.sync(folderPath.replace(/\\/g, "/") + "/**/*.sql");
        files.forEach(filePath => {
            // ignore dirs with *.sql name
            //   ./dir.sql/file
            const stat = fs.lstatSync(filePath);
            if ( !stat.isFile() ) {
                return;
            }

            let file: File | undefined;
            try {
                file = this.parseFile(folderPath, filePath);

                if ( file ) {
                    this.state.addFile(file);
                }
            } catch(err) {
                this.emitError({
                    subPath: filePath.slice( folderPath.length + 1 ),
                    err
                });
            }
        });
    }

    private emitError(params: {subPath: string, err: Error}) {
        const outError = prepareError(params.err, params.subPath);
        this.emit("error", outError);
    }

}
