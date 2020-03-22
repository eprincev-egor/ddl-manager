import {FSTest} from "../FSTest";

describe("FSDDLState", () => {

    it("load file with table and trigger inside one file", async() => {
        const test = new FSTest({
            "company.sql": [
                {
                    type: "function",
                    sql: `
                        create or replace function create_role()
                        returns trigger as $body$
                        begin
                            insert into roles (id_company, id_role)
                            values (new.id, 1)
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "public.create_role()",
                        name: "create_role"
                    }
                },
                {
                    type: "trigger",
                    sql: `
                        create trigger on create_role_trigger
                        after insert
                        on companies
                        for each row
                        execute procedure create_role()
                    `,
                    row: {
                        identify: "create_role_trigger on public.companies",
                        tableIdentify: "public.companies",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }
                },
                {
                    type: "table",
                    sql: `
                        create table company (
                            id serial primary key,
                            name text not null unique,
                            note text
                        )
                    `,
                    row: {
                        filePath: "test.sql",
                        identify: "public.companies",
                        name: "companies",
                        columns: [
                            {
                                filePath: "company.sql",
                                identify: "id",
                                key: "id",
                                type: "serial",
                                nulls: false,
                                parsed: null
                            },
                            {
                                filePath: "company.sql",
                                identify: "name",
                                key: "name",
                                type: "text",
                                nulls: false,
                                parsed: null
                            },
                            {
                                filePath: "company.sql",
                                identify: "note",
                                key: "note",
                                type: "text",
                                nulls: true,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: ["id"],
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        values: null,
                        parsed: null
                    }
                }
            ]
        });

        await test.testLoading({
            folder: {
                path: "./",
                name: "",
                files: [
                    test.getFileJSON("company.sql")
                ],
                folders: []
            },
            triggers: [
                {
                    filePath: "company.sql",
                    identify: "create_role_trigger on public.companies",
                    tableIdentify: "public.companies",
                    functionIdentify: "public.create_role()",
                    name: "create_role_trigger",
                    parsed: null,
                    createdByDDLManager: true
                }
            ],
            functions: [
                {
                    filePath: "company.sql",
                    identify: "public.create_role()",
                    name: "create_role",
                    parsed: null,
                    createdByDDLManager: true
                }
            ],
            tables: [
                {
                    filePath: "company.sql",
                    identify: "public.companies",
                    name: "companies",
                    columns: [
                        {
                            filePath: "company.sql",
                            identify: "id",
                            key: "id",
                            type: "serial",
                            nulls: false,
                            parsed: null
                        },
                        {
                            filePath: "company.sql",
                            identify: "name",
                            key: "name",
                            type: "text",
                            nulls: false,
                            parsed: null
                        },
                        {
                            filePath: "company.sql",
                            identify: "note",
                            key: "note",
                            type: "text",
                            nulls: true,
                            parsed: null
                        }
                    ],
                    deprecated: false,
                    deprecatedColumns: [],
                    primaryKey: ["id"],
                    checkConstraints: [],
                    foreignKeysConstraints: [],
                    uniqueConstraints: [],
                    values: null,
                    parsed: null
                }
            ],
            views: [],
            extensions: []
        });

    });
    

});