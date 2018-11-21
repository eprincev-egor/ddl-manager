"use strict";

const assert = require("assert");
const getDbClient = require("../utils/getDbClient");
const DdlManager = require("../../DdlManager");

describe("DdlManager.createObjectsTable", () => {

    it("create on empty database", async() => {
        let db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        await DdlManager.createObjectsTable(db);

        await db.query(`
            insert into ddl_manager_objects (
                identify, type, 
                ddl
            )
            values (
                'test()', 'function', 
                'create or replace function test() returns void as $$select 1$$ language sql;'
            )
        `);

        db.end();
    });

    it("do nothing, if table exists", async() => {
        let db = await getDbClient();

        await db.query(`
            drop schema public cascade;
            create schema public;

            create table ddl_manager_objects (
                identify text not null,
                type text not null check(
                    type in ('trigger', 'function')
                ),
                ddl text not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify, type)
            );
        `);

        await DdlManager.createObjectsTable(db);

        await db.query(`
            insert into ddl_manager_objects (
                identify, type, 
                ddl
            )
            values (
                'test()', 'function', 
                'create or replace function test() returns void as $$select 1$$ language sql;'
            )
        `);

        db.end();
    });

    it("throw error, if exists invalid table", async() => {
        let db = await getDbClient();
        const invalid_table_error = "please, drop table public.ddl_manager_objects";
        
        await db.query(`
            drop schema public cascade;
            create schema public;
        `);


        // no column 'identify'
        await db.query(`
            create table ddl_manager_objects (
                type text not null check(
                    type in ('trigger', 'function')
                ),
                ddl text not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (type)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "no column 'identify'"
            );
        }


        // no column 'type'
        await db.query(`
            drop table ddl_manager_objects;

            create table ddl_manager_objects (
                identify text not null,
                ddl text not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "no column 'type'"
            );
        }

        
        // no column 'ddl'
        await db.query(`
            drop table ddl_manager_objects;

            create table ddl_manager_objects (
                identify text not null,
                type text not null check(
                    type in ('trigger', 'function')
                ),
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify, type)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "no column 'ddl'"
            );
        }


        // wrong type for column 'identify'
        await db.query(`
            drop table ddl_manager_objects;
            
            create table ddl_manager_objects (
                identify bigint not null,
                type text not null check(
                    type in ('trigger', 'function')
                ),
                ddl text not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify, type)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "wrong type for column 'identify'"
            );
        }

        
        // wrong type for column 'type'
        await db.query(`
            drop table ddl_manager_objects;
            
            create table ddl_manager_objects (
                identify text not null,
                type bigint not null,
                ddl text not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify, type)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "wrong type for column 'type'"
            );
        }


        // wrong type for column 'ddl'
        await db.query(`
            drop table ddl_manager_objects;
            
            create table ddl_manager_objects (
                identify text not null,
                type text not null,
                ddl bigint not null,
                dt_create timestamp without time zone
                    not null default now(),
                dt_update timestamp without time zone,
                constraint ddl_manager_objects_pk primary key (identify, type)
            );
        `);
        
        try {
            await DdlManager.createObjectsTable(db);

            throw new Error("expected error");
        } catch(err) {
            assert.equal(
                err.message, invalid_table_error,
                "wrong type for column 'ddl'"
            );
        }


        db.end();
    });

});