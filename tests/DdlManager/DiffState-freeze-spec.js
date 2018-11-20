"use strict";

const assert = require("assert");
const DdlManager = require("../../DdlManager");

describe("DddlManager.diffState freeze tests", () => {

    it("simple freeze function in db, empty diff", () => {
        let freezeFunc = {
            freeze: true,
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };

        let diff = DdlManager.diffState({
            filesState: {
                functions: [],
                triggers: []
            },
            dbState: {
                functions: [
                    freezeFunc
                ],
                triggers: []
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [],
                functions: [
                    freezeFunc
                ]
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

    it("simple freeze function in db, error on replace", () => {
        let fileFunc = {
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };
        let freezeFunc = {
            freeze: true,
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };

        try {
            DdlManager.diffState({
                filesState: {
                    functions: [
                        fileFunc
                    ],
                    triggers: []
                },
                dbState: {
                    functions: [
                        freezeFunc
                    ],
                    triggers: []
                }
            });

            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "cannot replace freeze function public.some_test_func()");
        }
    });

    it("simple freeze trigger in db, empty diff", () => {
        let freezeFunc = {
            freeze: true,
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };
        let freezeTrigger = {
            freeze: true,
            table: {
                schema: "public",
                name: "company"
            },
            after: true,
            insert: true,
            name: "some_test_func_trigger",
            procedure: {
                schema: "public",
                name: "some_test_func"
            }
        };

        let diff = DdlManager.diffState({
            filesState: {
                functions: [],
                triggers: []
            },
            dbState: {
                functions: [
                    freezeFunc
                ],
                triggers: [
                    freezeTrigger
                ]
            }
        });

        assert.deepEqual(diff, {
            freeze: {
                triggers: [
                    freezeTrigger
                ],
                functions: [
                    freezeFunc
                ]
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


    it("simple freeze trigger in db, error on replace", () => {
        let func = {
            schema: "public",
            name: "some_test_func",
            args: [],
            returns: "integer",
            body: `begin
                return x + y;
            end`
        };
        let fileTrigger = {
            table: {
                schema: "public",
                name: "company"
            },
            after: true,
            insert: true,
            name: "some_test_func_trigger",
            procedure: {
                schema: "public",
                name: "some_test_func"
            }
        };
        let freezeTrigger = {
            freeze: true,
            table: {
                schema: "public",
                name: "company"
            },
            after: true,
            insert: true,
            name: "some_test_func_trigger",
            procedure: {
                schema: "public",
                name: "some_test_func"
            }
        };

        try {
            DdlManager.diffState({
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
                        freezeTrigger
                    ]
                }
            });

            throw new Error("expected error");
        } catch(err) {
            assert.equal(err.message, "cannot replace freeze trigger some_test_func_trigger on public.company");
        }
    });

});