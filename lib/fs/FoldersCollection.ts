import {Collection} from "model-layer";
import index from "./index";

export default class FoldersCollection extends Collection<FoldersCollection> {
    Model() {
        return index.FolderModel;
    }
}

index.FoldersCollection = FoldersCollection;