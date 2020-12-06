
export const TEST_FUNC1_SQL = `
    create or replace function test_func1()
    returns void as $body$select 1$body$
    language sql;
`;

export const TEST_FUNC1 = {
    language: "sql",
    schema: "public",
    name: "test_func1",
    args: [],
    returns: {type: "void"},
    body: "select 1"
};

export const TEST_FUNC2_SQL = `
    create or replace function test_func2()
    returns void as $body$select 2$body$
    language sql;
`;

export const TEST_FUNC2 = {
    language: "sql",
    schema: "public",
    name: "test_func2",
    args: [],
    returns: {type: "void"},
    body: "select 2"
};

export const TEST_FUNC3_SQL = `
    create or replace function test_func3()
    returns void as $body$select 3$body$
    language sql;
`;

export const TEST_FUNC3 = {
    language: "sql",
    schema: "public",
    name: "test_func3",
    args: [],
    returns: {type: "void"},
    body: "select 3"
};

const VOID_BODY = `begin
end`;

export const VOID_FUNC1_SQL = `
    create or replace function void_func1()
    returns void as $body$${VOID_BODY}$body$
    language plpgsql;
`.trim();

export const VOID_FUNC1 = {
    language: "plpgsql",
    schema: "public",
    name: "void_func1",
    args: [],
    returns: {type: "void"},
    body: VOID_BODY
};

export const VOID_FUNC2_SQL = `
    create or replace function void_func2()
    returns void as $body$${VOID_BODY}$body$
    language plpgsql;
`.trim();

export const VOID_FUNC2 = {
    language: "plpgsql",
    schema: "public",
    name: "void_func2",
    args: [],
    returns: {type: "void"},
    body: VOID_BODY
};