import fs from "fs";
import { SimpleLoader } from "./SimpleLoader";
import { TriggerDBO } from "../objects/TriggerDBO";

const sqlPath = __dirname + "/sql/select-triggers.sql";
const selectTriggersSQL = fs.readFileSync(sqlPath).toString();

export class FunctionsLoader
extends SimpleLoader<TriggerDBO> {

    getSelectObjectsSQL() {
        return selectTriggersSQL;
    }
}