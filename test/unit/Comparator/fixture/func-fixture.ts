import { DatabaseFunction } from "../../../../lib/database/schema/DatabaseFunction";

export const someFileParams = {
    name: "some_file.sql",
    folder: "/",
    path: "/some_file.sql"
};

export const someFuncParams = {
    schema: "public",
    name: "some_test_func",
    args: [
        {
            name: "x",
            type: "integer"
        },
        {
            name: "y",
            type: "integer"
        }
    ],
    returns: {type: "integer"},
    body: `begin
        return x + y;
    end`
};

export const testFunc = new DatabaseFunction({
    ...someFuncParams
});

export const testFileWithFunc = {
    ...someFileParams,
    content: {
        functions: [testFunc]
    }
};
