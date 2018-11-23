"use strict";

const assert = require("assert");
const fs = require("fs");
const FilesState = require("../../lib/FilesState");
const del = require("del");

describe("FilesState parse functions and triggers", () => {
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
        `;
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let expectedFunctions = [
            {
                language: "plpgsql",
                schema: "public",
                name: "some_action_on_diu_company",
                args: [],
                returns: "trigger",
                body
            }
        ];

        let expectedTriggers = [
            {
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
        ];
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let actualFunctions = filesState.getFunctions();
        let actualTriggers = filesState.getTriggers();

        assert.deepEqual(actualFunctions, expectedFunctions);
        assert.deepEqual(actualTriggers, expectedTriggers);

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
        `;
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        try {
            FilesState.create({
                folder: ROOT_TMP_PATH
            });

            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "wrong procedure name public.some_action_on_diu_company2");
        }
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
        `;
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        try {
            FilesState.create({
                folder: ROOT_TMP_PATH
            });

            assert.ok(false, "expected error");
        } catch(err) {
            assert.equal(err.message, "wrong returns type bigint");
        }

        fs.unlinkSync(filePath);
    });


});