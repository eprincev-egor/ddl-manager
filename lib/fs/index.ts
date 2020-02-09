import FolderModel from "./FolderModel";
import FoldersCollection from "./FoldersCollection";

const index: {
    FolderModel: new (...args: any[]) => FolderModel;
    FoldersCollection: new (...args: any[]) => FoldersCollection;
} = {} as any;

export default index;