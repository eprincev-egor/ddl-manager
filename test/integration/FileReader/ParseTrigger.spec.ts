import assert from "assert";
import fs from "fs";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { prepare } from "../utils/prepare";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileReader parse functions and triggers", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    prepare(ROOT_TMP_PATH);

    it("parse file with function and trigger", () => {
        const body = `
        begin
            return new;
        end
        `;

        const sql = `
            create or replace function some_action_on_diu_company()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company();
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const expectedFunctions = [
            {
                language: "plpgsql",
                schema: "public",
                name: "some_action_on_diu_company",
                args: [],
                returns: {type: "trigger"},
                body
            }
        ];

        const expectedTriggers = [
            {
                table: {
                    schema: "public",
                    name: "company"
                },
                name: "some_action_on_diu_company_trigger",
                after: true,
                insert: true,
                updateOf: ["deleted", "name"],
                delete: true,
                procedure: {
                    schema: "public",
                    name: "some_action_on_diu_company"
                }
            }
        ];
        
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });

        const actualFunctions = flatMap(filesReader.state.files, file => file.content.functions);
        const actualTriggers = flatMap(filesReader.state.files, file => file.content.triggers);

        expect(actualFunctions).to.be.shallowDeepEqual(expectedFunctions);
        expect(actualTriggers).to.be.shallowDeepEqual(expectedTriggers);

    });

    
    it("expected error on wrong procedure name", () => {
        const body = `
        begin
            return new;
        end
        `;

        const sql = `
            create or replace function some_action_on_diu_company1()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company2();
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let err = {message: "expected error"};
        FileReader.read({
            folder: ROOT_TMP_PATH,
            onError(_err: Error) {
                err = _err;
            }
        });

        assert.equal(err.message, "wrong procedure name public.some_action_on_diu_company2");
    });

    
    it("expected error on wrong returns type", () => {
        const body = `
        begin
            return new;
        end
        `;

        const sql = `
            create or replace function some_action_on_diu_company()
            returns bigint as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company();
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        let err = {message: "expected error"};
        FileReader.read({
            folder: ROOT_TMP_PATH,
            onError(_err: Error) {
                err = _err;
            }
        });

        assert.equal(err.message, "file must contain function with returns type trigger");
    });

    it("expected error on duplicate trigger", () => {
        const sql1 = `
            create or replace function func1()
            returns trigger as $body$select 1$body$
            language sql;

            create trigger some_trigger
            after insert
            on company
            for each row
            execute procedure func1();
        `;
        const sql2 = `
            create or replace function func2()
            returns trigger as $body$select 2$body$
            language sql;

            create trigger some_trigger
            after delete
            on company
            for each row
            execute procedure func2();
        `;
        
        const filePath1 = ROOT_TMP_PATH + "/func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/func2.sql";
        fs.writeFileSync(filePath1, sql1);
        fs.writeFileSync(filePath2, sql2);


        let err = {message: "expected error"};
        FileReader.read({
            folder: ROOT_TMP_PATH,
            onError(_err: Error) {
                err = _err;
            }
        });
        
        assert.equal(err.message, "duplicate trigger some_trigger on public.company");
        
    });

    it("parse trigger with comment", () => {
        const body = `
        begin
            return new;
        end
        `;

        const sql = `
            create or replace function some_action_on_diu_company()
            returns trigger as $body$${body}$body$
            language plpgsql;

            create trigger some_action_on_diu_company_trigger
            after insert or update of name, deleted or delete
            on company
            for each row
            execute procedure some_action_on_diu_company();

            comment on trigger some_action_on_diu_company_trigger on company is $$test$$;
        `;
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";
        fs.writeFileSync(filePath, sql);

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });

        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            {
                table: {
                    schema: "public",
                    name: "company"
                },
                name: "some_action_on_diu_company_trigger",
                after: true,
                insert: true,
                updateOf: ["deleted", "name"],
                delete: true,
                procedure: {
                    schema: "public",
                    name: "some_action_on_diu_company"
                },
                comment: "test"
            }
        ]);
    });


});