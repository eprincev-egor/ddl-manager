
export const TEST_FUNC1_SQL = `
    create or replace function test_func1()
    returns trigger as $body$select 1$body$
    language sql;

    create trigger some_trigger
    before insert
    on operation.company
    for each row
    execute procedure test_func1()
`;

export const TEST_FUNC1 = {
    language: "sql",
    schema: "public",
    name: "test_func1",
    args: [],
    returns: {type: "trigger"},
    body: "select 1"
};

export const TEST_TRIGGER1 = {
    table: {
        schema: "operation",
        name: "company"
    },
    name: "some_trigger",
    before: true,
    insert: true,
    procedure: {
        schema: "public",
        name: "test_func1"
    }
};

export const ONLY_FUNCTION_SQL = `
    create or replace function nice()
    returns bigint as $$select 1$$
    language sql;
`;

export const ONLY_FUNCTION = {
    language: "sql",
    schema: "public",
    name: "nice",
    args: [],
    returns: {type: "bigint"},
    body: "select 1"
};


export const TEST_FUNC2_SQL = `
    create or replace function some_func2()
    returns trigger as $body$select 2$body$
    language sql;

    create trigger some_trigger2
    before delete
    on operation.company
    for each row
    execute procedure some_func2()
`;

export const TEST_FUNC2 = {
    language: "sql",
    schema: "public",
    name: "some_func2",
    args: [],
    returns: {type: "trigger"},
    body: "select 2"
};

export const TEST_TRIGGER2 = {
    table: {
        schema: "operation",
        name: "company"
    },
    name: "some_trigger2",
    before: true,
    delete: true,
    procedure: {
        schema: "public",
        name: "some_func2"
    }
};
