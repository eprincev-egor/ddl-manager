import { DatabaseFunction } from "../../../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../../lib/database/schema/DatabaseTrigger";
import { TableID } from "../../../../lib/database/schema/TableID";

export const someFileParams = {
    name: "some_file.sql",
    folder: "/",
    path: "/some_file.sql"
};

export const someTriggerFuncParams = {
    schema: "public",
    name: "some_trigger_func",
    args: [],
    returns: {type: "trigger"},
    body: `begin
        return new;
    end`
};

export const someTriggerParams = {
    table: new TableID(
        "public",
        "company"
    ),
    after: true,
    insert: true,
    name: "some_trigger",
    procedure: {
        schema: "public",
        name: "some_trigger_func",
        args: []
    },
};

export const testTriggerFunc = new DatabaseFunction({
    ...someTriggerFuncParams
});

export const testTrigger = new DatabaseTrigger({
    ...someTriggerParams
});

export const testFileWithTrigger = {
    ...someFileParams,
    content: {
        functions: [testTriggerFunc],
        triggers: [testTrigger]
    }
};