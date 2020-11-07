import _ from "lodash";
import { DbState } from "../../lib/DbState";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";

use(chaiShallowDeepEqualPlugin);

function diffState(params: {filesState: any, dbState: any}) {
    const {dbState, filesState} = params;

    const state = new (DbState as any)();

    state.functions = dbState.functions;
    state.triggers = dbState.triggers;

    if ( dbState.comments ) {
        state.comments = dbState.comments;
    }

    return state.getDiff( filesState );
}

describe("DbState.getDiff", () => {

    it("sync empty state", () => {
        const diff = diffState({
            filesState: {
                functions: [],
                triggers: []
            },
            dbState: {
                functions: [],
                triggers: []
            }
        });

        expect(diff).to.be.shallowDeepEqual({
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
                ],
                triggers: []
            },
            dbState: {
                functions: [],
                triggers: []
            }
        });

        expect(diff).to.be.shallowDeepEqual({
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

        expect(diff).to.be.shallowDeepEqual({
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
        

        expect(diff).to.be.shallowDeepEqual({
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
        
        expect(diff).to.be.shallowDeepEqual({
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

        expect(diff).to.be.shallowDeepEqual({
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
                name: "some_action_on_some_event"
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
                name: "some_action_on_some_event"
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


        expect(diff).to.be.shallowDeepEqual({
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
                name: "some_action_on_some_event"
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


        expect(diff).to.be.shallowDeepEqual({
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
        const func = {
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return x + y;
            end`
        };
        const comment = {
            function: {
                schema: "public",
                name: "some_test_func",
                args: []
            },
            comment: {content: "test"}
        };

        const diff = diffState({
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

        expect(diff).to.be.shallowDeepEqual({
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
        const func = {
            schema: "public",
            name: "some_func",
            args: [],
            returns: {type: "integer"},
            body: `begin
                return 1;
            end`,
            freeze: true
        };

        const diff = diffState({
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

        expect(diff).to.be.shallowDeepEqual({
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
        const func = {
            schema: "public",
            name: "some_action_on_some_event",
            args: [],
            returns: {type: "trigger"},
            body: `begin
                return new;
            end`,
            freeze: true
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
                name: "some_action_on_some_event"
            },
            freeze: true
        };

        const diff = diffState({
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

        expect(diff).to.be.shallowDeepEqual({
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
                name: "some_action_on_some_event"
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

        expect(diff).to.be.shallowDeepEqual({
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

        expect(diff).to.be.shallowDeepEqual({
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