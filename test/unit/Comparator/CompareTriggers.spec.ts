import { MainComparator } from "../../../lib/Comparator/MainComparator";
import { Comment } from "../../../lib/database/schema/Comment";
import { Database } from "../../../lib/database/schema/Database";
import { DatabaseFunction } from "../../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../lib/database/schema/DatabaseTrigger";
import { Table } from "../../../lib/database/schema/Table";
import { FilesState } from "../../../lib/fs/FilesState";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import {
    someFileParams,
    someTriggerFuncParams,
    testTriggerFunc,
    testFileWithTrigger,
    testTrigger,
    someTriggerParams
} from "./fixture/trigger-fixture";

describe("Comparator: compare triggers", () => {

    let database!: Database;
    let fs!: FilesState;
    beforeEach(() => {
        database = new Database();
        fs = new FilesState();
    });

    it("sync empty state", () => {
        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });
    });

    it("create trigger", () => {

        fs.addFile(testFileWithTrigger);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    testTriggerFunc
                ],
                triggers: [
                    testTrigger
                ]
            }
        });
    });

    it("drop trigger", () => {

        database.addFunctions([testTriggerFunc]);
        database.setTable(new Table(testTrigger.table.schema, testTrigger.table.name));
        database.addTrigger(testTrigger);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: [
                    testTriggerFunc
                ],
                triggers: [
                    testTrigger
                ]
            },
            create: {
                functions: [],
                triggers: []
            }
        });
    });

    it("change trigger event type", () => {
        const fileTrigger = new DatabaseTrigger({
            ...someTriggerParams,
            before: false,
            after: true,
            insert: true
        });
        const dbTrigger = new DatabaseTrigger({
            ...someTriggerParams,
            before: true,
            after: false,
            insert: true
        });

        database.addFunctions([testTriggerFunc]);
        database.setTable(new Table(dbTrigger.table.schema, dbTrigger.table.name));
        database.addTrigger(dbTrigger);

        fs.addFile({
            ...someFileParams,
            content: {
                functions: [testTriggerFunc],
                triggers: [fileTrigger]
            }
        });

        const migration = MainComparator.compare(database, fs);


        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [
                    dbTrigger
                ],
                functions: []
            },
            create: {
                triggers: [
                    fileTrigger
                ],
                functions: []
            }
        });
    });


    it("no changes, same states, empty migration (triggers)", () => {
        
        database.addFunctions([testTriggerFunc]);
        database.setTable(new Table(testTrigger.table.schema, testTrigger.table.name));
        database.addTrigger(testTrigger);

        fs.addFile(testFileWithTrigger);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: []
            },
            create: {
                triggers: [],
                functions: []
            }
        });
    });

    it("empty migration on new frozen trigger", () => {
        const func = new DatabaseFunction({
            ...someTriggerFuncParams,
            comment: Comment.frozen("function")
        });
        const trigger = new DatabaseTrigger({
            ...someTriggerParams,
            comment: Comment.frozen("trigger")
        });

        database.addFunctions([func]);
        database.setTable(new Table(trigger.table.schema, trigger.table.name));
        database.addTrigger(trigger);


        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: []
            },
            create: {
                triggers: [],
                functions: []
            }
        });
    });


    it("drop trigger, if function has change, but trigger not", () => {
        const func1 = new DatabaseFunction({
            ...someTriggerFuncParams,
            body: `begin
                return new;
            end`
        });
        const func2 = new DatabaseFunction({
            ...someTriggerFuncParams,
            body: `begin
                -- some change
                return new;
            end`
        });

        database.addFunctions([func1]);
        database.setTable(new Table(testTrigger.table.schema, testTrigger.table.name));
        database.addTrigger(testTrigger);

        fs.addFile({
            ...someFileParams,
            content: {
                functions: [func2],
                triggers: [testTrigger]
            }
        });

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [
                    testTrigger
                ],
                functions: [
                    func1
                ]
            },
            create: {
                triggers: [
                    testTrigger
                ],
                functions: [
                    func2
                ]
            }
        });
    });

    it("trigger with long name inside fs and db", () => {
        const longName = "long_name_0123456789012345678901234567890123456789012345678901234567890123456789";

        const triggerInFS = new DatabaseTrigger({
            ...someTriggerParams,
            name: longName
        });
        const triggerInDB = new DatabaseTrigger({
            ...someTriggerParams,
            name: longName.slice(0, 64)
        });

        database.addFunctions([testTriggerFunc]);
        database.setTable(new Table(triggerInDB.table.schema, triggerInDB.table.name));
        database.addTrigger(triggerInDB);

        fs.addFile({
            ...someFileParams,
            content: {
                functions: [testTriggerFunc],
                triggers: [triggerInFS]
            }
        });

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: []
            },
            create: {
                triggers: [],
                functions: []
            }
        });
    });

});