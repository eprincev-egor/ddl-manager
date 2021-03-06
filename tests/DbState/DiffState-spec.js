"use strict";

const assert = require("assert");
const _ = require("lodash");
const DbState = require("../../lib/DbState");

function diffState({filesState, dbState}) {
    let state = new DbState();

    state.functions = dbState.functions;
    state.triggers = dbState.triggers;

    if ( dbState.comments ) {
        state.comments = dbState.comments;
    }

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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "integer"},
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
            returns: {type: "trigger"},
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
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`
        };
        let dbFunc = _.cloneDeep(func);

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

    it("create function with comment", () => {
        let func = {
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };
        let comment = {
            function: {
                schema: "public",
                name: "some_test_func",
                args: []
            },
            comment: "test"
        };

        let diff = diffState({
            filesState: {
                functions: [
                    func
                ],
                triggers: [],
                comments: [
                    comment
                ]
            },
            dbState: {
                functions: [],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            drop: {
                triggers: [],
                functions: []
            },
            create: {
                triggers: [],
                functions: [
                    func
                ],
                comments: [
                    comment
                ]
            }
        });
    });

    it("empty diff on new freeze function", () => {
        let func = {
            schema: "public",
            name: "some_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return 1;
            end`,
            freeze: true
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

    it("empty diff on new freeze trigger", () => {
        let func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`,
            freeze: true
        };
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
            },
            freeze: true
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
                triggers: [
                    trigger
                ]
            }
        });

        assert.deepEqual(diff, {
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
        let func1 = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`
        };
        let func2 = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                -- some change
                return new;
            end`
        };
        let trigger1 = {
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
        let trigger2 = {
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

        assert.deepEqual(diff, {
            drop: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func2
                ]
            },
            create: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func1
                ]
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

        assert.deepEqual(diff, {
            drop: {
                triggers: [
                    trigger1
                ],
                functions: [
                    func2
                ]
            },
            create: {
                triggers: [
                    trigger2
                ],
                functions: [
                    func1
                ]
            }
        });
    });
});