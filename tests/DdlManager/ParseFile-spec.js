"use strict";

const assert = require("assert");
const fs = require("fs");
const DdlManager = require("../../DdlManager");

const ROOT_TMP_PATH = __dirname + "/tmp";

before(() => {
    if ( !fs.existsSync(ROOT_TMP_PATH) ) {
        fs.mkdirSync(ROOT_TMP_PATH);
    }
});

describe("DdlManager.parseFile", () => {
    
    it("parse nonexistent file", () => {

        try {
            DdlManager.parseFile("---");
            
            assert.ok(false, "expected error for nonexistent file");
        } catch(err) {
            assert.equal(err.message, "file \"---\" not found");
        }
    });

    it("parse empty file", () => {
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, "");

        let result = DdlManager.parseFile(filePath);

        assert.strictEqual(result, null);

        fs.unlinkSync(filePath);
    });

    it("parse file with simple function", () => {
        let body = `
        begin
            return a + b;
        end
        `;

        let sql = `
            create or replace function public.test_a_plus_b(
                a bigint,
                b bigint
            )
            returns bigint as $body$${body}$body$
            language plpgsql;
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = {
            function: {
                language: "plpgsql",
                schema: "public",
                name: "test_a_plus_b",
                args: [
                    {
                        name: "a",
                        type: "bigint"
                    },
                    {
                        name: "b",
                        type: "bigint"
                    }
                ],
                returns: "bigint",
                body
            }
        };
        
        let actualResult = DdlManager.parseFile(filePath);

        assert.deepEqual(actualResult, expectedResult);

        fs.unlinkSync(filePath);
    });

    it("parse file with simple sql function", () => {
        let sql = `
            create or replace function nice_sql()
            returns integer as $body$select 1$body$
            language sql;
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = {
            function: {
                schema: "public",
                name: "nice_sql",
                args: [],
                returns: "integer",
                language: "sql",
                body: "select 1"
            }
        };
        
        let actualResult = DdlManager.parseFile(filePath);

        assert.deepEqual(actualResult, expectedResult);

        fs.unlinkSync(filePath);
    });

    it("parse file with simple function and returns table", () => {
        let body = `
        begin
            return next;
        end
        `;

        let sql = `
            create or replace function public.test_a_plus_b()
            returns table(
                name text,
                total numeric(10, 3 )
            ) as $body$${body}$body$
            language plpgsql;
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = {
            function: {
                language: "plpgsql",
                schema: "public",
                name: "test_a_plus_b",
                args: [],
                returns: {
                    table: [
                        {
                            name: "name",
                            type: "text"
                        },
                        {
                            name: "total",
                            type: "numeric(10,3)"
                        }
                    ]
                },
                body
            }
        };
        
        let actualResult = DdlManager.parseFile(filePath);

        assert.deepEqual(actualResult, expectedResult);

        fs.unlinkSync(filePath);
    });

    it("parse file with function and trigger", () => {
        let body = `
        begin
            return new;
        end
        `;

        let sql = `
            create or replace function some_action_on_diu_company()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company();
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = {
            function: {
                language: "plpgsql",
                schema: "public",
                name: "some_action_on_diu_company",
                args: [],
                returns: "trigger",
                body
            },
            trigger: {
                table: {
                    schema: "public",
                    name: "company"
                },
                name: "some_action_on_diu_company_trigger",
                after: true,
                insert: true,
                update: ["name", "deleted"],
                delete: true,
                procedure: {
                    schema: "public",
                    name: "some_action_on_diu_company"
                }
            }
        };
        
        let actualResult = DdlManager.parseFile(filePath);

        assert.deepEqual(actualResult, expectedResult);

        fs.unlinkSync(filePath);
    });

    it("expected error on wrong procedure name", () => {
        let body = `
        begin
            return new;
        end
        `;

        let sql = `
            create or replace function some_action_on_diu_company1()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company2();
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        try {
            DdlManager.parseFile(filePath);

            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "wrong procedure name public.some_action_on_diu_company2");
        }

        fs.unlinkSync(filePath);
    });

    it("expected error on wrong returns type", () => {
        let body = `
        begin
            return new;
        end
        `;

        let sql = `
            create or replace function some_action_on_diu_company()
            returns bigint as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company();
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        try {
            DdlManager.parseFile(filePath);

            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "wrong returns type bigint");
        }

        fs.unlinkSync(filePath);
    });

});