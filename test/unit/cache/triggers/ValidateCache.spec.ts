import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { Database } from "../../../../lib/database/schema/Database";
import assert from "assert";

describe("TriggerFabric, validate cache", () => {

    it("error on: order by without limit 1", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select orders.date as order_date
                from orders
                order by orders.id
            )
        `, new Database([]));

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /required limit 1/.test(err.message)
        );
    });

    it("error on: limit 1 without order by", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select orders.date as order_date
                from orders
                limit 1
            )
        `, new Database([]));

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /required order by/.test(err.message)
        );
    });

    it("error on: invalid limit 1", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select orders.date as order_date
                from orders
                order by orders.id
                limit 100
            )
        `, new Database([]));

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /invalid limit: 100, limit can be only 1/.test(err.message)
        );
    });

    it("error on: many items order by", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select orders.date as order_date
                from orders
                order by orders.id, orders.date
                limit 1
            )
        `, new Database([]));

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /order by many items is not supported/.test(err.message)
        );
    });

    it("error on: order by joined tables", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select orders.date as order_date
                from orders

                left join order_type on
                    order_type.id = orders.id_type

                order by order_type.code
                limit 1
            )
        `, new Database([]));

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /order by joined table "public\.order_type" is not supported/.test(err.message)
        );
    });
});