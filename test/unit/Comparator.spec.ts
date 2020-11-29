import _ from "lodash";
import { Comparator } from "../../lib/Comparator";
import { FileParser } from "../../lib/parser/FileParser";
import assert from "assert";
import {
    Cache,
    DatabaseFunction,
    DatabaseTrigger,
    IDatabaseFunctionParams,
    IDatabaseTriggerParams
} from "../../lib/ast";
import { Database } from "../../lib/database/schema/Database";
import { Table as DBTable } from "../../lib/database/schema/Table";
import { Diff } from "../../lib/Diff";
interface IStateParams {
    functions: (DatabaseFunction | IDatabaseFunctionParams)[];
    triggers: (DatabaseTrigger | IDatabaseTriggerParams)[];
    cache: Cache[];
}

function diffState(params: {filesState: Partial<IStateParams>, dbState: Partial<IStateParams>}) {
    const {
        dbState: dbStateParams,
        filesState: filesStateParams
    } = params;


    const database = new Database([]);
    database.addFunctions(createFuncsInstances(dbStateParams));

    for (const trigger of createTriggersInstances(dbStateParams)) {
        const table = new DBTable(
            trigger.table.schema,
            trigger.table.name
        );
        database.setTable(table);
        database.addTrigger( trigger );
    }

    const filesState = {
        functions: createFuncsInstances(filesStateParams),
        triggers: createTriggersInstances(filesStateParams),
        cache: filesStateParams.cache || []
    };

    const diff = Comparator.compare(database, filesState);
    return diff;
}


function deepStrictEqualDiff(
    actualDiff: Diff,
    expectedDiffParams: Partial<{
        create: Partial<IStateParams>;
        drop: Partial<IStateParams>;
    }>
) {
    
    const expectedCreate: Partial<IStateParams> = expectedDiffParams.create || {};
    const expectedDrop: Partial<IStateParams> = expectedDiffParams.drop || {};

    const expectedDiff = Diff.empty()
        .create({
            functions: createFuncsInstances(expectedCreate),
            triggers: createTriggersInstances(expectedCreate),
            cache: expectedCreate.cache
        })
        .drop({
            functions: createFuncsInstances(expectedDrop),
            triggers: createTriggersInstances(expectedDrop),
            cache: expectedDrop.cache
        })
    ;
    
    assert.deepStrictEqual(actualDiff, expectedDiff);
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
        const diff = diffState({
            filesState: {
            },
            dbState: {
            }
        });

        deepStrictEqualDiff(diff, {
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

        const diff = diffState({
            filesState: {
                functions: [
                    func
                ]
            },
            dbState: {
            }
        });

        deepStrictEqualDiff(diff, {
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
        const diff = diffState({
            filesState: {
            },
            dbState: {
                functions: [
                    func
                ]
            }
        });

        deepStrictEqualDiff(diff, {
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

        const diff = diffState({
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
        

        deepStrictEqualDiff(diff, {
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

        const diff = diffState({
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
        
        deepStrictEqualDiff(diff, {
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

    it("no changes, same states, empty diff", () => {
        const func = {
            schema: "public",
            name: "some_test_func3",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return 10;
            end`
        };

        const diff = diffState({
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

        deepStrictEqualDiff(diff, {
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
            table: {
                schema: "public",
                name: "company"
            },
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
            table: {
                schema: "public",
                name: "company"
            },
            before: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };

        const diff = diffState({
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


        deepStrictEqualDiff(diff, {
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

    it("no changes, same states, empty diff (triggers)", () => {
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
            table: {
                schema: "public",
                name: "company"
            },
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

        const diff = diffState({
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


        deepStrictEqualDiff(diff, {
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

        const diff = diffState({
            filesState: {
                functions: [
                    func
                ]
            },
            dbState: {
            }
        });

        deepStrictEqualDiff(diff, {
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

    it("empty diff on new frozen function", () => {
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

        const diff = diffState({
            filesState: {
            },
            dbState: {
                functions: [
                    func
                ]
            }
        });

        deepStrictEqualDiff(diff, {
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

    it("empty diff on new frozen trigger", () => {
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
            table: {
                schema: "public",
                name: "company"
            },
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

        const diff = diffState({
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

        deepStrictEqualDiff(diff, {
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
            table: {
                schema: "public",
                name: "company"
            },
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
            table: {
                schema: "public",
                name: "company"
            },
            before: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event",
                args: []
            }
        };

        let diff;

        // for drop function, need drop trigger, who call it function
        diff = diffState({
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

        deepStrictEqualDiff(diff, {
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


        // check diff on duplicate trigger
        // when we have changes in trigger and function
        diff = diffState({
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

        deepStrictEqualDiff(diff, {
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

});