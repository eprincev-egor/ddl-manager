import {TestState, ITestFiles} from "../TestState";


describe("FSState", () => {

    it("load dir with one file with trigger", async() => {
        const files: ITestFiles = {
            "./create_role.sql": [
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
                }
            ]
        };

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "./create_role.sql",
                            name: "create_role.sql",
                            content: TestState.concatFilesSql( files["./create_role.sql"] )
                        }
                    ],
                    folders: []
                },
                triggers: [
                    {
                        filePath: "./create_role.sql",
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
                        filePath: "./create_role.sql",
                        identify: "public.create_role()",
                        name: "create_role",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                tables: [],
                views: []
            }
        });

    });
    

    it("load dir with one file with two triggers", async() => {
        const files: ITestFiles = {
            "./role_trigger.sql": [
                {
                    type: "function",
                    sql: `
                        create or replace function create_role()
                        returns trigger as $body$
                        begin
                            insert into roles (id_company, id_role)
                            values (new.id, 1);

                            return new;
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
                    type: "function",
                    sql: `
                        create or replace function delete_role()
                        returns trigger as $body$
                        begin
                            delete from roles 
                            where id_company = old.id;
                            
                            return old;
                        end
                        $body$
                        language plpgsql;
                    `,
                    row: {
                        identify: "public.delete_role()",
                        name: "delete_role"
                    }
                },
                {
                    type: "trigger",
                    sql: `
                        create trigger on create_role_trigger
                        after insert
                        on companies
                        for each row
                        execute procedure create_role();
                    `,
                    row: {
                        identify: "create_role_trigger on public.companies",
                        tableIdentify: "public.companies",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger"
                    }
                },
                {
                    type: "trigger",
                    sql: `
                        create trigger on delete_role_trigger
                        before delete
                        on companies
                        for each row
                        execute procedure delete_role();
                    `,
                    row: {
                        identify: "delete_role_trigger on public.companies",
                        tableIdentify: "public.companies",
                        functionIdentify: "public.delete_role()",
                        name: "delete_role_trigger"
                    }
                }
            ]
        };

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "./role_trigger.sql",
                            name: "role_trigger.sql",
                            content: TestState.concatFilesSql( files["./role_trigger.sql"] )
                        }
                    ],
                    folders: []
                },
                triggers: [
                    {
                        filePath: "./role_trigger.sql",
                        identify: "create_role_trigger on public.companies",
                        tableIdentify: "public.companies",
                        functionIdentify: "public.create_role()",
                        name: "create_role_trigger",
                        parsed: null,
                        createdByDDLManager: true
                    },
                    {
                        filePath: "./role_trigger.sql",
                        identify: "delete_role_trigger on public.companies",
                        tableIdentify: "public.companies",
                        functionIdentify: "public.delete_role()",
                        name: "delete_role_trigger",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                functions: [
                    {
                        filePath: "./role_trigger.sql",
                        identify: "public.create_role()",
                        name: "create_role",
                        parsed: null,
                        createdByDDLManager: true
                    },
                    {
                        filePath: "./role_trigger.sql",
                        identify: "public.delete_role()",
                        name: "delete_role",
                        parsed: null,
                        createdByDDLManager: true
                    }
                ],
                tables: [],
                views: []
            }
        });

    });
    

});