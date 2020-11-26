import fs from "fs";

export const CM_ARRAY_REMOVE_ONE_ELEMENT = fs.readFileSync(
    __dirname + "/cm_array_remove_one_element.sql"
).toString();

export const CM_ARRAY_TO_STRING_DISTINCT = fs.readFileSync(
    __dirname + "/cm_array_to_string_distinct.sql"
).toString();

export const CM_DISTINCT_ARRAY = fs.readFileSync(
    __dirname + "/cm_distinct_array.sql"
).toString();