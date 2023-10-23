import assert from "assert";
import fs from "fs";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { prepare } from "../utils/prepare";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileReader parse functions", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    prepare(ROOT_TMP_PATH);

    it("parse nonexistent file", () => {

        try {
            FileReader.read(["---"]);
            
            assert.ok(false, "expected error for nonexistent file");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("parse empty folder", () => {

        const state = FileReader.read([ROOT_TMP_PATH]);

        const functions = state.allNotHelperFunctions();

        expect(functions).to.deep.equal([]);
    });

    
    it("parse file with simple function", () => {
        const body = `
        begin
            return a + b;
        end
        `;

        const sql = `
            create or replace function public.test_a_plus_b(
                a bigint,
                b bigint
            )
            returns bigint as $body$${body}$body$
            language plpgsql;
        `.trim();
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedResult = [
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
                returns: {type: "bigint"},
                body
            }
        ];

        const state = FileReader.read([ROOT_TMP_PATH]);

        const actualResult = state.allNotHelperFunctions();

        expect(actualResult).to.be.shallowDeepEqual(expectedResult);
    });

    
    it("parse file with simple sql function", () => {
        const sql = `
            create or replace function nice_sql()
            returns integer as $body$select 1$body$
            language sql;
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedResult = [
            {
                schema: "public",
                name: "nice_sql",
                args: [],
                returns: {type: "integer"},
                language: "sql",
                body: "select 1"
            }
        ];
        
        const state = FileReader.read([ROOT_TMP_PATH]);

        const actualResult = state.allNotHelperFunctions();

        expect(actualResult).to.be.shallowDeepEqual(expectedResult);
    });

    
    it("parse file with simple function and returns table", () => {
        const body = `
        begin
            return next;
        end
        `;

        const sql = `
            create or replace function public.test_a_plus_b()
            returns table(
                name text,
                total numeric(10, 3 )
            ) as $body$${body}$body$
            language plpgsql;
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedResult = [
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
                            type: "numeric(10, 3)"
                        }
                    ]
                },
                body
            }
        ];
        
        const state = FileReader.read([ROOT_TMP_PATH]);

        const actualResult = state.allNotHelperFunctions();

        expect(actualResult).to.be.shallowDeepEqual(expectedResult);
    });



    it("expected error on duplicated functions", () => {
        const sql1 = `
            create or replace function func1()
            returns bigint as $body$select 1$body$
            language sql;
        `;
        const sql2 = `
            create or replace function func1()
            returns integer as $body$select 2$body$
            language sql;
        `;
        
        const filePath1 = ROOT_TMP_PATH + "/func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/func2.sql";
        fs.writeFileSync(filePath1, sql1);
        fs.writeFileSync(filePath2, sql2);

        let err = {message: "expected error"};
        FileReader.read([ROOT_TMP_PATH], (_err: Error) => {
            err = _err;
        });

        assert.equal(err.message, "duplicated function public.func1()");
    });

    it("same functions, but another args", () => {
        const sql1 = `
            create or replace function func1()
            returns bigint as $body$select 1$body$
            language sql;
        `;
        const func1 = {
            schema: "public",
            name: "func1",
            args: [],
            returns: {type: "bigint"},
            language: "sql",
            body: "select 1"
        };
        const sql2 = `
            create or replace function func1(a text)
            returns integer as $body$select 2$body$
            language sql;
        `;
        const func2 = {
            schema: "public",
            name: "func1",
            args: [
                {
                    name: "a",
                    type: "text"
                }
            ],
            returns: {type: "integer"},
            language: "sql",
            body: "select 2"
        };
        
        
        const filePath1 = ROOT_TMP_PATH + "/func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/func2.sql";
        fs.writeFileSync(filePath1, sql1);
        fs.writeFileSync(filePath2, sql2);

        const state = FileReader.read([ROOT_TMP_PATH]);
        
        expect(state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            func1,
            func2
        ]);
    });

    
    it("parse file with comments", () => {
        const body = `
        begin
            return a + b;
        end
        `;

        const sql = `
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
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedResult = [
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
                returns: {type: "bigint"},
                body
            }
        ];

        const state = FileReader.read([ROOT_TMP_PATH]);

        const actualResult = state.allNotHelperFunctions();

        expect(actualResult).to.be.shallowDeepEqual(expectedResult);
    });

    it("do not replace comments inside function", () => {
        const body = `
        begin
            -- some comment
            return 1;
        end
        `;

        const sql = `
            create or replace function public.func_with_comment()
            returns bigint as $body$${body}$body$
            language plpgsql;
        `.trim();
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedResult = [
            {
                language: "plpgsql",
                schema: "public",
                name: "func_with_comment",
                args: [],
                returns: {type: "bigint"},
                body
            }
        ];

        const state = FileReader.read([ROOT_TMP_PATH]);

        const actualResult = state.allNotHelperFunctions();

        expect(actualResult).to.be.shallowDeepEqual(expectedResult);
    });

});