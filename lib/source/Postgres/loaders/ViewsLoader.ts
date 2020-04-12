import fs from "fs";
import { SimpleLoader } from "./SimpleLoader";
import { ViewDBO } from "../objects/ViewDBO";

const sqlPath = __dirname + "/sql/select-views.sql";
const selectViewsSQL = fs.readFileSync(sqlPath).toString();

export class ViewsLoader
extends SimpleLoader<ViewDBO> {

    getSelectObjectsSQL() {
        return selectViewsSQL;
    }
}