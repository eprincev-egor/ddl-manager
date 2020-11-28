import { DatabaseFunction } from "../../lib/ast";
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

})