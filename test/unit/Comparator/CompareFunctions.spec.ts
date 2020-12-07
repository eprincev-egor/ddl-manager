import _ from "lodash";
import { MainComparator } from "../../../lib/Comparator/MainComparator";
import { Comment } from "../../../lib/database/schema/Comment";
import { Database } from "../../../lib/database/schema/Database";
import { DatabaseFunction } from "../../../lib/database/schema/DatabaseFunction";
import { FilesState } from "../../../lib/fs/FilesState";
import { deepStrictEqualMigration } from "./deepStrictEqualMigration";
import {
    someFileParams,
    someFuncParams,
    testFunc,
    testFileWithFunc
} from "./fixture/func-fixture";

describe("Comparator: compare functions", () => {
    
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
                functions: []
            },
            create: {
                functions: []
            }
        });
    });

    it("create simple function", () => {
        
        fs.addFile(testFileWithFunc);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: []
            },
            create: {
                functions: [
                    testFunc
                ]
            }
        });
    });

    it("drop function", () => {
        database.addFunctions([ testFunc ]);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: [
                    testFunc
                ]
            },
            create: {
                functions: []
            }
        });
    });

    it("replace function", () => {
        const fileFunc = new DatabaseFunction({
            ...someFuncParams,
            body: `begin
                return x * y;
            end`
        });

        const dbFunc = new DatabaseFunction({
            ...someFuncParams,
            body: `begin
                return x + y;
            end`
        });

        database.addFunctions([dbFunc]);
        fs.addFile({
            ...someFileParams,
            content: {
                functions: [fileFunc]
            }
        });

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: [
                    dbFunc
                ]
            },
            create: {
                functions: [
                    fileFunc
                ]
            }
        });
    });

    it("replace function, change arguments length", () => {
        const fileFunc = new DatabaseFunction({
            ...someFuncParams,
            args: [
                {
                    name: "a",
                    type: "integer"
                },
                {
                    name: "b",
                    type: "integer"
                },
                {
                    name: "c",
                    type: "integer"
                }
            ]
        });

        const dbFunc = new DatabaseFunction({
            ...someFuncParams,
            args: [
                
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ]
        });

        database.addFunctions([dbFunc]);
        fs.addFile({
            ...someFileParams,
            content: {
                functions: [fileFunc]
            }
        });

        const migration = MainComparator.compare(database, fs);
        
        deepStrictEqualMigration(migration, {
            drop: {
                functions: [
                    dbFunc
                ]
            },
            create: {
                functions: [
                    fileFunc
                ]
            }
        });
    });

    it("no changes, same states, empty migration", () => {

        database.addFunctions([testFunc]);
        fs.addFile(testFileWithFunc);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: []
            },
            create: {
                functions: []
            }
        });
    });

    it("create function with comment", () => {
        const func = new DatabaseFunction({
            ...someFuncParams,
            comment: "test"
        });
        fs.addFile({
            ...someFileParams,
            content: {
                functions: [func]
            }
        });

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: []
            },
            create: {
                functions: [
                    func
                ]
            }
        });
    });

    it("empty migration on frozen function in db", () => {
        const func = new DatabaseFunction({
            ...someFuncParams,
            comment: Comment.frozen("function")
        });
        database.addFunctions([func]);

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: []
            },
            create: {
                functions: []
            }
        });
    });

    it("function with long name inside fs and db", () => {
        const longName = "long_name_0123456789012345678901234567890123456789012345678901234567890123456789";

        const funcInFS = new DatabaseFunction({
            ...someFuncParams,
            name: longName
        });
        const funcInDB = new DatabaseFunction({
            ...someFuncParams,
            name: longName.slice(0, 64)
        });

        database.addFunctions([funcInDB]);
        fs.addFile({
            ...someFileParams,
            content: {
                functions: [funcInFS]
            }
        });

        const migration = MainComparator.compare(database, fs);

        deepStrictEqualMigration(migration, {
            drop: {
                functions: []
            },
            create: {
                functions: []
            }
        });
    });


});