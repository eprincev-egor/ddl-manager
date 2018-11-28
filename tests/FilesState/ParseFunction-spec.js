"use strict";

const assert = require("assert");
const fs = require("fs");
const FilesState = require("../../lib/FilesState");
const del = require("del");

describe("FilesState parse functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        del.sync(ROOT_TMP_PATH);
    });

    it("parse nonexistent file", () => {

        try {
            FilesState.create({
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent file");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("parse empty folder", () => {

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let functions = filesState.getFunctions();

        assert.deepEqual(functions, []);
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

        let expectedResult = [
            {
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
        ];

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let actualResult = filesState.getFunctions();

        assert.deepEqual(actualResult, expectedResult);
    });

    
    it("parse file with simple sql function", () => {
        let sql = `
            create or replace function nice_sql()
            returns integer as $body$select 1$body$
            language sql;
        `;
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = [
            {
                schema: "public",
                name: "nice_sql",
                args: [],
                returns: "integer",
                language: "sql",
                body: "select 1"
            }
        ];
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let actualResult = filesState.getFunctions();

        assert.deepEqual(actualResult, expectedResult);
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
        `;
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = [
            {
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
        ];
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let actualResult = filesState.getFunctions();

        assert.deepEqual(actualResult, expectedResult);
    });



    it("expected error on duplicate functions", () => {
        let sql1 = `
            create or replace function func1()
            returns bigint as $body$select 1$body$
            language sql;
        `;
        let sql2 = `
            create or replace function func1()
            returns integer as $body$select 2$body$
            language sql;
        `;
        
        let filePath1 = ROOT_TMP_PATH + "/func1.sql";
        let filePath2 = ROOT_TMP_PATH + "/func2.sql";
        fs.writeFileSync(filePath1, sql1);
        fs.writeFileSync(filePath2, sql2);

        let err = {message: "expected error"};
        FilesState.create({
            folder: ROOT_TMP_PATH,
            onError(_err) {
                err = _err;
            }
        });

        assert.equal(err.message, "duplicate function public.func1()");
    });

    it("same functions, but another args", () => {
        let sql1 = `
            create or replace function func1()
            returns bigint as $body$select 1$body$
            language sql;
        `;
        let func1 = {
            schema: "public",
            name: "func1",
            args: [],
            returns: "bigint",
            language: "sql",
            body: "select 1"
        };
        let sql2 = `
            create or replace function func1(a text)
            returns integer as $body$select 2$body$
            language sql;
        `;
        let func2 = {
            schema: "public",
            name: "func1",
            args: [
                {
                    name: "a",
                    type: "text"
                }
            ],
            returns: "integer",
            language: "sql",
            body: "select 2"
        };
        
        
        let filePath1 = ROOT_TMP_PATH + "/func1.sql";
        let filePath2 = ROOT_TMP_PATH + "/func2.sql";
        fs.writeFileSync(filePath1, sql1);
        fs.writeFileSync(filePath2, sql2);

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            func1,
            func2
        ]);
    });

    
    it("parse file with comments", () => {
        let body = `
        begin
            return a + b;
        end
        `;

        let sql = `
            -- some comment here
            /*
                and here
            */
           
            create or replace function public.test_a_plus_b(
                a bigint,
                b bigint
            )
            returns bigint as $body$${body}$body$
            language plpgsql;
        `.trim();
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedResult = [
            {
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
        ];

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let actualResult = filesState.getFunctions();

        assert.deepEqual(actualResult, expectedResult);
    });

});