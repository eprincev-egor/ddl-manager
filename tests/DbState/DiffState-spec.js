"use strict";

const assert = require("assert");
const _ = require("lodash");
const DbState = require("../../lib/DbState");

function diffState({filesState, dbState}) {
    let state = new DbState();

    state.functions = dbState.functions;
    state.triggers = dbState.triggers;

    return state.getDiff( filesState );
}

describe("DbState.getDiff", () => {

    it("sync empty state", () => {
        let diff = diffState({
            filesState: {
                functions: [],
                triggers: []
            },
            dbState: {
                functions: [],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
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

    it("create simple function", () => {
        let func = {
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
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };

        let diff = diffState({
            filesState: {
                functions: [
                    func
                ],
                triggers: []
            },
            dbState: {
                functions: [],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
            drop: {
                triggers: [],
                functions: []
            },
            create: {
                triggers: [],
                functions: [
                    func
                ]
            }
        });
    });

    it("drop function", () => {
        let func = {
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
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };
        let diff = diffState({
            filesState: {
                functions: [],
                triggers: []
            },
            dbState: {
                functions: [
                    func
                ],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
            drop: {
                triggers: [],
                functions: [
                    func
                ]
            },
            create: {
                triggers: [],
                functions: []
            }
        });
    });

    it("replace function", () => {
        let fileFunc = {
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
            returns: "integer",
            body: `begin
                return x * y;
            end`
        };

        let dbFunc = {
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
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };

        let diff = diffState({
            filesState: {
                functions: [
                    fileFunc
                ],
                triggers: []
            },
            dbState: {
                functions: [
                    dbFunc
                ],
                triggers: []
            }
        });
        

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
            drop: {
                triggers: [],
                functions: [
                    dbFunc
                ]
            },
            create: {
                triggers: [],
                functions: [
                    fileFunc
                ]
            }
        });
    });

    it("replace function, change arguments length", () => {
        let fileFunc = {
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
            returns: "integer",
            body: `begin
                return a + b + c;
            end`
        };

        let dbFunc = {
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
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };

        let diff = diffState({
            filesState: {
                functions: [
                    fileFunc
                ],
                triggers: []
            },
            dbState: {
                functions: [
                    dbFunc
                ],
                triggers: []
            }
        });
        
        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
            drop: {
                triggers: [],
                functions: [
                    dbFunc
                ]
            },
            create: {
                triggers: [],
                functions: [
                    fileFunc
                ]
            }
        });
    });

    it("no changes, same states, empty diff", () => {
        let func = {
            schema: "public",
            name: "some_test_func3",
            args: [],
            returns: "integer",
            body: `begin
                return 10;
            end`
        };

        let diff = diffState({
            filesState: {
                functions: [
                    func
                ],
                triggers: []
            },
            dbState: {
                functions: [
                    func
                ],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
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

    it("change trigger event type", () => {
        let func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: "trigger",
            body: `begin
                return new;
            end`
        };
        let fileTrigger = {
            table: {
                schema: "public",
                name: "company"
            },
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event"
            }
        };
        let dbTrigger = {
            table: {
                schema: "public",
                name: "company"
            },
            before: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event"
            }
        };

        let diff = diffState({
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


        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
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

    it("no changes, same states, empty diff (triggers)", () => {
        let func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: "trigger",
            body: `begin
                return new;
            end`
        };
        let dbFunc = _.cloneDeep(func);
        dbFunc.freeze = false;

        let trigger = {
            table: {
                schema: "public",
                name: "company"
            },
            after: true,
            insert: true,
            name: "some_action_on_some_event_trigger",
            procedure: {
                schema: "public",
                name: "some_action_on_some_event"
            }
        };
        let dbTrigger = _.cloneDeep(trigger);
        dbTrigger.freeze = false;

        let diff = diffState({
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


        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: []
            },
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