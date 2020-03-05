import PgParser from "../../../lib/parser/PgParser";
import assert from "assert";
import ViewModel from "../../../lib/objects/ViewModel";
import FunctionModel from "../../../lib/objects/FunctionModel";
import TriggerModel from "../../../lib/objects/TriggerModel";
import TableModel from "../../../lib/objects/TableModel";

describe("PgParser", () => {

    describe("commented View", () => {

        it("ignore inline comment before view", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- hello
                create view hello_view as
                    select 1;
                
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof ViewModel, 
                "instanceof ViewModel"
            );
        });
        
        it("ignore inline comment after view", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create view hello_view as
                    select 1
                -- hello
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof ViewModel, 
                "instanceof ViewModel"
            );
        });
        
        it("ignore inline-commented view", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- create view hello_view as
                --     select 1
            `);
            assert.ok(result.length === 0, "result.length === 0");
        });
        
        it("ignore inline inside view", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create view hello_view as
                    -- select
                     select 1
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof ViewModel, 
                "instanceof ViewModel"
            );
        });

    });

    describe("commented Function", () => {

        it("ignore inline comment after function", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create or replace function test()
                returns void as $body$
                begin
                end
                $body$
                language plpgsql;
                -- hello
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof FunctionModel, 
                "instanceof FunctionModel"
            );
        });
        
        it("ignore inline comment before function", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- hello
                create or replace function test()
                returns void as $body$
                begin
                end
                $body$
                language plpgsql;
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof FunctionModel, 
                "instanceof FunctionModel"
            );
        });
        
        it("ignore inline comment inside function", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create or replace function test(
                    -- hi
                )
                returns void as $body$
                -- comment
                begin
                end
                $body$
                language plpgsql;
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof FunctionModel, 
                "instanceof FunctionModel"
            );
        });
    
        it("ignore inline-commented function", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- create or replace function test(
                -- )
                -- returns void as $body$
                -- begin
                -- end
                -- $body$
                -- language plpgsql;
            `);
            assert.ok(result.length === 0, "result.length === 0");
        });
        
    });

    describe("commented Trigger", () => {

        it("ignore inline comment after trigger", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create trigger nice_trigger
                after insert
                on company
                for each row 
                execute procedure test();
                -- hello
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof TriggerModel, 
                "instanceof TriggerModel"
            );
        });
        
        it("ignore inline comment before trigger", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- hello
                create trigger nice_trigger
                after insert
                on company
                for each row 
                execute procedure test();
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof TriggerModel, 
                "instanceof TriggerModel"
            );
        });
        
        it("ignore inline comment inside trigger", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create trigger nice_trigger
                -- some
                after insert
                -- comment
                on company
                -- here
                for each row 
                -- and here
                execute procedure test();
            `);
            assert.ok(result.length === 1, "result.length === 1");

            assert.ok(
                result[0] instanceof TriggerModel, 
                "instanceof TriggerModel"
            );
        });
    
        it("ignore inline-commented trigger", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- create trigger nice_trigger
                -- after insert
                -- on company
                -- for each row 
                -- execute procedure test();
            `);
            assert.ok(result.length === 0, "result.length === 0");
        });
        
    });

    describe("commented Table", () => {

        it("ignore inline comment before table", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- hello
                create table company (
                    id serial primary key,
                    name text not null unique
                )
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof TableModel, 
                "instanceof TableModel"
            );
        });
        
        it("ignore inline comment after table", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                create table company (
                    id serial primary key,
                    name text not null unique
                )
                -- hello
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof TableModel, 
                "instanceof TableModel"
            );
        });
        
        it("ignore inline-commented table", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- create table company (
                --     id serial primary key,
                --     name text not null unique
                -- )
            `);
            assert.ok(result.length === 0, "result.length === 0");
        });
        
        it("ignore inline inside table", async() => {
            const parser = new PgParser();
            
            const result = parser.parseFile("test.sql", `
                -- comment
                create table company (
                    -- and here comment
                    id serial primary key,
                    -- and here
                    name text not null unique
                    -- you know
                )
            `);
            assert.ok(result.length === 1, "result.length === 1");
    
            assert.ok(
                result[0] instanceof TableModel, 
                "instanceof TableModel"
            );
        });

    });
     
});