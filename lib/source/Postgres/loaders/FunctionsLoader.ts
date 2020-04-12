import fs from "fs";
import { SimpleLoader } from "./SimpleLoader";
import { FunctionDBO } from "../objects/FunctionDBO";

const sqlPath = __dirname + "/sql/select-functions.sql";
const selectFunctionsSQL = fs.readFileSync(sqlPath).toString();

export class FunctionsLoader
extends SimpleLoader<FunctionDBO> {

    getSelectObjectsSQL() {
        return selectFunctionsSQL;
    }
}