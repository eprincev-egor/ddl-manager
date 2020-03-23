import {FolderModel} from "./FolderModel";
import {FoldersCollection} from "./FoldersCollection";

const index: {
    FolderModel: new (...args: any[]) => FolderModel;
    FoldersCollection: new (...args: any[]) => FoldersCollection;
} = {} as any;

// tslint:disable-next-line: no-default-export
export default index;