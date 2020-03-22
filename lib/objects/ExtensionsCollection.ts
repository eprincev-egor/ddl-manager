import BaseDBObjectCollection from "./BaseDBObjectCollection";
import ExtensionModel from "./ExtensionModel";

export default class ExtensionsCollection extends BaseDBObjectCollection<ExtensionsCollection> {
    Model() {
        return ExtensionModel;
    }

    findExtensionsForTable(tableIdentify: string): ExtensionModel[] {
        return this.models.filter(extension =>
            extension.get("forTableIdentify") === tableIdentify
        );
    }
}