import { DatabaseFunction, IDatabaseFunctionArgument } from "../../lib/ast";
import assert from "assert";

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

    it("equal with different frozen", () => {
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
            frozen: true
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            frozen: false
        });
        
        assert.ok( !func1.equal(func2), "func1 != func2" );
        assert.ok( !func2.equal(func1), "func2 != func1" );
    });

    it("equal different: null == false == undefined", () => {
        const func1 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            frozen: null as any
        });

        const func2 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            frozen: false
        });
        
        const func3 = new DatabaseFunction({
            schema: "public",
            name: "my_func",
            args: [],
            returns: {type: "bigint"},
            body: "body",
            frozen: undefined as any
        });

        assert.ok( func1.equal(func2), "func1 == func2" );
        assert.ok( func2.equal(func3), "func2 == func3" );
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
                    type: "timestamp without time zone",
                    default: "null :: timestamp without time zone "
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

})