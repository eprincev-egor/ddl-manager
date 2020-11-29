import _ from "lodash";
import { Comparator } from "../../lib/Comparator";
import assert from "assert";
import { Cache } from "../../lib/ast";
import { Database } from "../../lib/database/schema/Database";
import { DatabaseFunction, IDatabaseFunctionParams } from "../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger, IDatabaseTriggerParams } from "../../lib/database/schema/DatabaseTrigger";
import { Table } from "../../lib/database/schema/Table";
import { TableID } from "../../lib/database/schema/TableID";
import { Migration } from "../../lib/Migrator/Migration";
import { FilesState } from "../../lib/fs/FilesState";
import { File } from "../../lib/fs/File";
interface IStateParams {
    functions: (DatabaseFunction | IDatabaseFunctionParams)[];
    triggers: (DatabaseTrigger | IDatabaseTriggerParams)[];
    cache: Cache[];
}

function compare(params: {filesState: Partial<IStateParams>, dbState: Partial<IStateParams>}) {
    const {
        dbState: dbStateParams,
        filesState: filesStateParams
    } = params;


    const database = new Database([]);
    database.addFunctions(createFuncsInstances(dbStateParams));

    for (const trigger of createTriggersInstances(dbStateParams)) {
        const table = new Table(
            trigger.table.schema,
            trigger.table.name
        );
        database.setTable(table);
        database.addTrigger( trigger );
    }

    const filesState = new FilesState();
    for (const func of createFuncsInstances(filesStateParams)) {
        filesState.addFile(new File({
            name: func.name + ".sql",
            folder: "",
            path: func.name + ".sql",
            content: {
                functions: [func],
                triggers: [],
                cache: []
            }
        }));
    }

    for (const trigger of createTriggersInstances(filesStateParams)) {
        filesState.addFile(new File({
            name: trigger.name + ".sql",
            folder: "",
            path: trigger.name + ".sql",
            content: {
                functions: [],
                triggers: [trigger],
                cache: []
            }
        }));
    }

    const migration = Comparator.compare(database, filesState);
    return migration;
}


function deepStrictEqualMigration(
    actualMigration: Migration,
    expectedMigrationParams: Partial<{
        create: Partial<IStateParams>;
        drop: Partial<IStateParams>;
    }>
) {
    
    const expectedCreate: Partial<IStateParams> = expectedMigrationParams.create || {};
    const expectedDrop: Partial<IStateParams> = expectedMigrationParams.drop || {};

    const expectedDiff = Migration.empty()
        .create({
            functions: createFuncsInstances(expectedCreate),
            triggers: createTriggersInstances(expectedCreate),
            // cache: expectedCreate.cache
        })
        .drop({
            functions: createFuncsInstances(expectedDrop),
            triggers: createTriggersInstances(expectedDrop),
            // cache: expectedDrop.cache
        })
    ;
    
    assert.deepStrictEqual(actualMigration, expectedDiff);
}

function createFuncsInstances(state: Partial<IStateParams>) {
    return (state.functions || []).map(funcParams =>
        funcParams instanceof DatabaseFunction ?
            funcParams :
            new DatabaseFunction(funcParams)
    );
}

function createTriggersInstances(state: Partial<IStateParams>) {
    return (state.triggers || []).map(triggerParams =>
        triggerParams instanceof DatabaseTrigger ?
            triggerParams :
            new DatabaseTrigger(triggerParams)
    );
}

describe("Comparator", () => {

    it("sync empty state", () => {
        const migration = compare({
            filesState: {
            },
            dbState: {
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("create simple function", () => {
        const func = {
            schema: "public",
            name: "some_test_func",
            args: [
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };

        const migration = compare({
            filesState: {
                functions: [
                    func
                ]
            },
            dbState: {
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [
                    func
                ],
                cache: []
            }
        });
    });

    it("drop function", () => {
        const func = {
            schema: "public",
            name: "some_test_func1",
            args: [
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };
        const migration = compare({
            filesState: {
            },
            dbState: {
                functions: [
                    func
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [
                    func
                ],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("replace function", () => {
        const fileFunc = {
            schema: "public",
            name: "some_test_func2",
            args: [
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x * y;
            end`
        };

        const dbFunc = {
            schema: "public",
            name: "some_test_func2",
            args: [
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };

        const migration = compare({
            filesState: {
                functions: [
                    fileFunc
                ]
            },
            dbState: {
                functions: [
                    dbFunc
                ]
            }
        });
        

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [
                    dbFunc
                ],
                cache: []
            },
            create: {
                triggers: [],
                functions: [
                    fileFunc
                ],
                cache: []
            }
        });
    });

    it("replace function, change arguments length", () => {
        const fileFunc = {
            schema: "public",
            name: "some_test_func3",
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
            ],
            returns: {type: "integer"},
            body: `begin
                return a + b + c;
            end`
        };

        const dbFunc = {
            schema: "public",
            name: "some_test_func3",
            args: [
                
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };

        const migration = compare({
            filesState: {
                functions: [
                    fileFunc
                ]
            },
            dbState: {
                functions: [
                    dbFunc
                ]
            }
        });
        
        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [
                    dbFunc
                ],
                cache: []
            },
            create: {
                triggers: [],
                functions: [
                    fileFunc
                ],
                cache: []
            }
        });
    });

    it("no changes, same states, empty migration", () => {
        const func = {
            schema: "public",
            name: "some_test_func3",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return 10;
            end`
        };

        const migration = compare({
            filesState: {
                functions: [
                    func
                ]
            },
            dbState: {
                functions: [
                    func
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("change trigger event type", () => {
        const func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`
        };
        const fileTrigger = {
            table: new TableID(
                "public",
                "company"
            ),
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };
        const dbTrigger = {
            table: new TableID(
                "public",
                "company"
            ),
            before: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };

        const migration = compare({
            filesState: {
                functions: [
                    func
                ],
                triggers: [
                    fileTrigger
                ]
            },
            dbState: {
                functions: [
                    func
                ],
                triggers: [
                    dbTrigger
                ]
            }
        });


        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [
                    dbTrigger
                ],
                functions: [],
                cache: []
            },
            create: {
                triggers: [
                    fileTrigger
                ],
                functions: [],
                cache: []
            }
        });
    });

    it("no changes, same states, empty migration (triggers)", () => {
        const func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`
        };
        const dbFunc = _.cloneDeep(func);

        const trigger = {
            table: new TableID(
                "public",
                "company"
            ),
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };
        const dbTrigger = _.cloneDeep(trigger);

        const migration = compare({
            filesState: {
                functions: [
                    func
                ],
                triggers: [
                    trigger
                ]
            },
            dbState: {
                functions: [
                    dbFunc
                ],
                triggers: [
                    dbTrigger
                ]
            }
        });


        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("create function with comment", () => {
        const func = {
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`,
            comment: "test"
        };

        const migration = compare({
            filesState: {
                functions: [
                    func
                ]
            },
            dbState: {
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [
                    func
                ],
                cache: []
            }
        });
    });

    it("empty migration on new frozen function", () => {
        const func = {
            schema: "public",
            name: "some_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return 1;
            end`,
            frozen: true
        };

        const migration = compare({
            filesState: {
            },
            dbState: {
                functions: [
                    func
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("empty migration on new frozen trigger", () => {
        const func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`,
            frozen: true
        };
        const trigger = {
            table: new TableID(
                "public",
                "company"
            ),
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            },
            frozen: true
        };

        const migration = compare({
            filesState: {
            },
            dbState: {
                functions: [
                    func
                ],
                triggers: [
                    trigger
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("drop trigger, if function has change, but trigger not", () => {
        const func1 = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`
        };
        const func2 = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                -- some change
                return new;
            end`
        };
        const trigger1 = {
            table: new TableID(
                "public",
                "company"
            ),
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };
        const trigger2 = {
            table: new TableID(
                "public",
                "company"
            ),
            before: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };

        let migration;

        // for drop function, need drop trigger, who call it function
        migration = compare({
            filesState: {
                functions: [
                    func1
                ],
                triggers: [
                    trigger1
                ]
            },
            dbState: {
                functions: [
                    func2
                ],
                triggers: [
                    trigger1
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func2
                ],
                cache: []
            },
            create: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func1
                ],
                cache: []
            }
        });


        // check migration on duplicate trigger
        // when we have changes in trigger and function
        migration = compare({
            filesState: {
                functions: [
                    func1
                ],
                triggers: [
                    trigger2
                ]
            },
            dbState: {
                functions: [
                    func2
                ],
                triggers: [
                    trigger1
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func2
                ],
                cache: []
            },
            create: {
                triggers: [
                    trigger2
                ],
                functions: [
                    func1
                ],
                cache: []
            }
        });
    });

    it("function with long name inside fs and db", () => {
        const someFuncParams = {
            schema: "public",
            args: [
                {
                    name: "x",
                    type: "integer"
                },
                {
                    name: "y",
                    type: "integer"
                }
            ],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };
        const longName = "long_name_0123456789012345678901234567890123456789012345678901234567890123456789";

        const funcInFS = new DatabaseFunction({
            ...someFuncParams,
            name: longName
        });
        const funcInDB = new DatabaseFunction({
            ...someFuncParams,
            name: longName.slice(0, 64)
        });

        const migration = compare({
            filesState: {
                functions: [
                    funcInFS
                ]
            },
            dbState: {
                functions: [
                    funcInDB
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });


    it("trigger with long name inside fs and db", () => {
        const someTriggerParams = {
            table: new TableID(
                "public",
                "company"
            ),
            after: true,
            insert: true,
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };

        const longName = "long_name_0123456789012345678901234567890123456789012345678901234567890123456789";

        const triggerInFS = new DatabaseTrigger({
            ...someTriggerParams,
            name: longName
        });
        const triggerInDB = new DatabaseTrigger({
            ...someTriggerParams,
            name: longName.slice(0, 64)
        });

        const migration = compare({
            filesState: {
                triggers: [
                    triggerInFS
                ]
            },
            dbState: {
                triggers: [
                    triggerInDB
                ]
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });
    /*

    it("create cache", () => {
        const cache = FileParser.parseCache(`
            cache test_cache for y (
                select
                    array_agg(y.id) as y_ids
                from y
            )
        `);

        const migration = compare({
            filesState: {
                cache: [cache]
            },
            dbState: {
            }
        });

        deepStrictEqualMigration(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: [cache]
            }
        });
    });

    it("drop cache", () => {
        const cache = FileParser.parseCache(`
            cache test_cache for y (
                select
                    array_agg(y.id) as y_ids
                from y
            )
        `);

        const migration = diffState({
            filesState: {
            },
            dbState: {
                cache: [cache]
            }
        });

        deepStrictEqualDiff(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: [cache]
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    it("empty migration if cache equals", () => {
        const cache = FileParser.parseCache(`
            cache test_cache for y (
                select
                    array_agg(y.id) as y_ids
                from y
            )
        `);

        const migration = diffState({
            filesState: {
                cache: [cache]
            },
            dbState: {
                cache: [cache]
            }
        });

        deepStrictEqualDiff(migration, {
            drop: {
                triggers: [],
                functions: [],
                cache: []
            },
            create: {
                triggers: [],
                functions: [],
                cache: []
            }
        });
    });

    */
});