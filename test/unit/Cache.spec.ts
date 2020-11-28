import { FileParser } from "../../lib/parser/FileParser";
import assert from "assert";

describe("DatabaseTrigger", () => {

    it("equal two similar caches", () => {
        const cache1 = FileParser.parseCache(`
            cache totals for companies (
                select count(*) as orders_count
                from orders
            )
        `);

        const cache2 = FileParser.parseCache(`
            cache totals for companies (
                select count(*) as orders_count
                from orders
            )
        `);
        
        assert.ok( cache1.equal(cache2), "cache1 == cache2" );
        assert.ok( cache2.equal(cache1), "cache2 == cache1" );
    });

    it("equal with different name", () => {
        const cache1 = FileParser.parseCache(`
            cache totals1 for companies (
                select count(*) as orders_count
                from orders
            )
        `);

        const cache2 = FileParser.parseCache(`
            cache totals2 for companies (
                select count(*) as orders_count
                from orders
            )
        `);
        
        assert.ok( !cache1.equal(cache2), "cache1 != cache2" );
        assert.ok( !cache2.equal(cache1), "cache2 != cache1" );
    });

    it("equal with different forTable", () => {
        const cache1 = FileParser.parseCache(`
            cache totals for companies1 (
                select count(*) as orders_count
                from orders
            )
        `);

        const cache2 = FileParser.parseCache(`
            cache totals for companies2 (
                select count(*) as orders_count
                from orders
            )
        `);
        
        assert.ok( !cache1.equal(cache2), "cache1 != cache2" );
        assert.ok( !cache2.equal(cache1), "cache2 != cache1" );
    });

    it("equal with different select", () => {
        const cache1 = FileParser.parseCache(`
            cache totals for companies (
                select count(*) as orders_count
                from orders1
            )
        `);

        const cache2 = FileParser.parseCache(`
            cache totals for companies (
                select count(*) as orders_count
                from orders2
            )
        `);
        
        assert.ok( !cache1.equal(cache2), "cache1 != cache2" );
        assert.ok( !cache2.equal(cache1), "cache2 != cache1" );
    });

    it("equal with different Without triggers", () => {
        const cache1 = FileParser.parseCache(`
            cache totals1 for companies (
                select count(*) as orders_count
                from orders
                left join order_type on true
            )
            without triggers on order_type
        `);

        const cache2 = FileParser.parseCache(`
            cache totals2 for companies (
                select count(*) as orders_count
                from orders
            )
        `);
        
        assert.ok( !cache1.equal(cache2), "cache1 != cache2" );
        assert.ok( !cache2.equal(cache1), "cache2 != cache1" );
    });

})