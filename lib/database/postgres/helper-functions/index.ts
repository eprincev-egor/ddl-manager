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

export const CM_EQUAL_ARRAYS = fs.readFileSync(
    __dirname + "/cm_equal_arrays.sql"
).toString();

export const CM_GET_INSERTED_ELEMENTS = fs.readFileSync(
    __dirname + "/cm_get_inserted_elements.sql"
).toString();

export const CM_GET_DELETED_ELEMENTS = fs.readFileSync(
    __dirname + "/cm_get_deleted_elements.sql"
).toString();

export const CM_ARRAY_APPEND_ORDER_BY_ASC_NULLS_LAST = fs.readFileSync(
    __dirname + "/cm_array_append_order_by_asc_nulls_last.sql"
).toString();

export const CM_ARRAY_APPEND_ORDER_BY_ASC_NULLS_FIRST = fs.readFileSync(
    __dirname + "/cm_array_append_order_by_asc_nulls_first.sql"
).toString();

export const CM_ARRAY_APPEND_ORDER_BY_DESC_NULLS_LAST = fs.readFileSync(
    __dirname + "/cm_array_append_order_by_desc_nulls_last.sql"
).toString();

export const CM_ARRAY_APPEND_ORDER_BY_DESC_NULLS_FIRST = fs.readFileSync(
    __dirname + "/cm_array_append_order_by_desc_nulls_first.sql"
).toString();
