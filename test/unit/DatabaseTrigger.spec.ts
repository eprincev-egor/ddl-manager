import { DatabaseTrigger } from "../../lib/ast";
import assert from "assert";

describe("DatabaseTrigger", () => {

    it("equal two similar triggers", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });
        
        assert.ok( trigger1.equal(trigger2), "trigger1 == trigger2" );
        assert.ok( trigger2.equal(trigger1), "trigger2 == trigger1" );
    });

    it("equal with different names", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger_1",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger_2",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });

    it("equal with different tables", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table_1"
            }
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table_2"
            }
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });


    it("equal with different funcs", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function_1",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function_2",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            }
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });


    it("equal with different events", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },

            before: true,
            insert: true
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },

            after: true,
            delete: true
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });

    it("equal with different 'when'", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            when: "when 1"
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            when: "when 2"
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });

    it("equal with different comment", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            comment: "comment 1"
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            comment: "comment 2"
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });

    it("equal comment: null == undefined", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            comment: null as any
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            comment: undefined as any
        });
        
        assert.ok( trigger1.equal(trigger2), "trigger1 == trigger2" );
        assert.ok( trigger2.equal(trigger1), "trigger2 == trigger1" );
    });

    it("equal with different frozen", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            frozen: false
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            frozen: true
        });
        
        assert.ok( !trigger1.equal(trigger2), "trigger1 != trigger2" );
        assert.ok( !trigger2.equal(trigger1), "trigger2 != trigger1" );
    });

    it("equal comment: null == undefined == false", () => {
        const trigger1 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            frozen: null as any
        });

        const trigger2 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            frozen: undefined as any
        });
        
        const trigger3 = new DatabaseTrigger({
            name: "my_trigger",
            procedure: {
                schema: "public",
                name: "my_trigger_function",
                args: []
            },
            table: {
                schema: "public",
                name: "my_table"
            },
            frozen: false
        });
        
        assert.ok( trigger1.equal(trigger2), "trigger1 == trigger2" );
        assert.ok( trigger2.equal(trigger3), "trigger2 == trigger3" );
    });

})