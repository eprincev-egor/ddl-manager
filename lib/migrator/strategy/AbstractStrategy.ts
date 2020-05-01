import { Migration } from "../Migration";
import { IDBO } from "../../common";
import { IChanges } from "../../utils/compare";

export abstract class AbstractStrategy<TObject extends IDBO> {
    abstract buildMigration(migration: Migration, changes: IChanges<TObject>);
}