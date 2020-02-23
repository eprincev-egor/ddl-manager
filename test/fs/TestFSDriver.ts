import FSDriver, {IDirContent} from "../../lib/fs/FSDriver";

export interface ITestDir {
    files: {[key: string]: string};
    folders: {[key: string]: ITestDir};
};

export default class TestFSDriver extends FSDriver {
    rootDir: ITestDir;

    constructor(testDir: ITestDir) {
        super();
        this.rootDir = testDir;
    }

    private findDir(folderPath): ITestDir {
        const names = folderPath
            .split(/[\\\/]/g)
            .slice(0, -1)
            .filter(name => 
                name !== "" &&
                name !== "."
            )
            .join("/");
        
        let dir = this.rootDir;
        for (const name of names) {
            dir = dir.folders[ name ];
        }

        return dir;
    }

    async readFile(filePath: string): Promise<string> {
        const pathElems = filePath
            .split(/[\\\/]/g)
            .filter(name => 
                name !== "" &&
                name !== "."
            );
        const folderPath = pathElems.slice(0, -1).join("/");
        const fileName = pathElems.pop();

        const dir = this.findDir(folderPath);

        return dir.files[ fileName ];
    }

    async readFolder(folderPath: string): Promise<IDirContent> {
        const dir = this.findDir(folderPath);
        const files = Object.keys(dir.files);
        const folders = Object.keys( dir.folders );

        return {
            files,
            folders
        };
    }
}