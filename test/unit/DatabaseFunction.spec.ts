import { DatabaseFunction, IDatabaseFunctionArgument } from "../../lib/database/schema/DatabaseFunction";
import assert from "assert";
import { Comment } from "../../lib/database/schema/Comment";

describe("DatabaseFunction", () => {

    it("equal two similar functions", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "begin\nend"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "begin\nend"
        });
        
        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func1), "func2 == func1" );
    });

    it("equal with different body", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body 1"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body 2"
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal with different args", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [{name: "hello", type: "bigint"}],
            returns: {type: "bigint"},
            body: "body"
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal with different comment", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            comment: "comment 1"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [{name: "hello", type: "bigint"}],
            returns: {type: "bigint"},
            body: "body",
            comment: "comment 2"
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal comment: undefined == null", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            comment: null as any
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            comment: undefined as any
        });
        
        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func1), "func2 == func1" );
    });

    it("equal with different returns", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "integer"},
            body: "body"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body"
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal with different returns type", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "integer"},
            body: "body"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body"
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal with different frozen", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            comment: Comment.frozen("function")
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            comment: Comment.empty("function")
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal args with similar default", () => {
        interface IArgCompare {
            argA: IDatabaseFunctionArgument;
            argB: IDatabaseFunctionArgument;
            equal: boolean;
        }
        const argsCompares: IArgCompare[] = [
            {
                argA: {
                    name: "_start_date",
                    type: "timestamp without time zone",
                    default: " null"
                },
                argB: {
                    name: "_start_date",
                    type: "timestamp without time zone",
                    default: "null :: timestamp without time zone"
                }, 
                equal: true
            },
            {
                argA: {
                    name: "_start_date",
                    type: "timestamp without time zone",
                    default: " null"
                },
                argB: {
                    name: "_start_date",
                    type: "timestamp without time zone",
                    default: "null "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "_start_date",
                    type: "timestamp without time zone",
                    default: "null::timestamp without time zone"
                },
                argB: {
                    name: "_start_date",
                    type: "timestamp WITHOUT time zone",
                    default: "null :: timestamp without time zone "
                }, 
                equal: true
            },

            {
                argA: {
                    name: "_start_date",
                    type: "timestamp ",
                    default: "null::timestamp "
                },
                argB: {
                    name: "_start_date",
                    type: "timestamp without  time zone",
                    default: "null :: timestamp without time zone "
                }, 
                equal: true
            },


            {
                argA: {
                    name: "_start_date",
                    type: "numeric",
                    default: "null::NUMERIc(14, 2)"
                },
                argB: {
                    name: "_start_date",
                    type: "numeric",
                    default: "null :: numeric"
                }, 
                equal: true
            },

            {
                argA: {
                    name: "name",
                    type: "text",
                    default: "'' :: text"
                },
                argB: {
                    name: "name",
                    type: "text",
                    default: " '' "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "jsonb",
                    default: "{} :: jsonb"
                },
                argB: {
                    name: "name",
                    type: "jsonb",
                    default: " {} "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "jsonb",
                    default: "'{}' :: jsonb"
                },
                argB: {
                    name: "name",
                    type: "jsonb",
                    default: " '{}' "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "jsonb",
                    default: "{} :: jsonb"
                },
                argB: {
                    name: "name",
                    type: "jsonb",
                    default: " '{}' "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "jsonb",
                    default: "'{}'"
                },
                argB: {
                    name: "name",
                    type: "jsonb",
                    default: " {} "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "boolean",
                    default: "false :: boolean"
                },
                argB: {
                    name: "name",
                    type: "boolean",
                    default: " false "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "boolean",
                    default: "false :: boolean"
                },
                argB: {
                    name: "name",
                    type: "boolean",
                    default: " false "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "boolean",
                    default: "false ::boolean"
                },
                argB: {
                    name: "name",
                    type: "boolean",
                    default: " false "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "smallint",
                    default: "0 :: smallint"
                },
                argB: {
                    name: "name",
                    type: "smallint",
                    default: " 0 "
                }, 
                equal: true
            },
            {
                argA: {
                    name: "name",
                    type: "text",
                    default: "'both' :: text"
                },
                argB: {
                    name: "name",
                    type: "text",
                    default: " 'both' "
                }, 
                equal: true
            }
        ];

        for (const {argA, argB, equal} of argsCompares) {
            const func1 = new DatabaseFunction({
                schema: "public",
                name: "my_func",
                args: [argA],
                returns: {type: "bigint"},
                body: "body"
            });
    
            const func2 = new DatabaseFunction({
                schema: "public",
                name: "my_func",
                args: [argB],
                returns: {type: "bigint"},
                body: "body"
            });
            
            assert.strictEqual(
                func1.equal(func2),
                equal,
                `equal("${argA.default}", "${argB.default}") => ${equal}`
            );
        }
        
    });

    // soo, maybe need another way?
    it("equal cost: 100 == null", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            cost: 100 as any
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            cost: null as any
        });
        
        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func1), "func2 == func1" );
    });

    it("equal arg type: numeric(14, 2) == numeric", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [{
                name: "test",
                type: "numeric(14,2)"
            }],
            returns: {type: "numeric(14,2)"},
            body: "body"
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [{
                name: "test",
                type: "numeric"
            }],
            returns: {type: "numeric"},
            body: "body"
        });
        
        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func1), "func2 == func1" );
    });

    it("equal two functions similar comments", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "begin\nend",
            comment: Comment.fromFs({
                objectType: "function",
                dev: "hello"
            })
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "begin\nend",
            comment: Comment.fromTotalString("function", "hello\nddl-manager-sync")
        });
        
        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func1), "func2 == func1" );
    });

    describe("findAssignColumns", () => {

        it("empty function", () => {
            shouldBeAssignedColumns(
                "begin\nend",
                []
            );
        });

        it("assign one column", () => {
            shouldBeAssignedColumns(`
            begin
                new.my_column = 123;
            end`, ["my_column"]);
        });

        it("ignore case", () => {
            shouldBeAssignedColumns(`
            begin
                new.MY_COLUMN = 123;
            end`, ["my_column"]);
        });

        it("assign two columns", () => {
            shouldBeAssignedColumns(`
            begin
                new.column_a = 123;
                new.column_b = 123;
            end`, ["column_a", "column_b"]);
        });

        it("dont repeat columns", () => {
            shouldBeAssignedColumns(`
            begin
                new.column_x = 123;
                new.column_x = 123;
            end`, ["column_x"]);
        });

        it("ignore if condition", () => {
            shouldBeAssignedColumns(`
            begin
                if new.column_x = 1 or new.column_y = 1 then
                    new.column_z = 123;
                end if;
            end`, ["column_z"]);
        });

        it("assign inside loop", () => {
            shouldBeAssignedColumns(`
            begin
                for ... loop
                    new.column_x = 19;
                end loop;
            end`, ["column_x"]);
        });

        it("ignore inline comments before assign", () => {
            shouldBeAssignedColumns(`
            begin
                -- comment
                new.column_x = 19;
            end`, ["column_x"]);
        });

        it("ignore multiline comments before assign", () => {
            shouldBeAssignedColumns(`
            begin
                /* comment */
                new.column_x = 19;
            end`, ["column_x"]);
        });


        function shouldBeAssignedColumns(
            body: string, columns: string[]
        ) {
            const func = new DatabaseFunction({
                schema: "public",
                name: "my_func",
                args: [],
                returns: {type: "bigint"},
                body
            });

            assert.deepStrictEqual(
                func.findAssignColumns(),
                columns
            );
        }

    });

})