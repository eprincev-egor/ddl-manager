import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { Database } from "../../../../lib/database/schema/Database";
import assert from "assert";

describe("TriggerFactory, no triggers for some table", () => {

    it("no triggers by table name", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select
                    string_agg(distinct order_type.name, ', ') as orders_types_names

                from orders

                left join order_type on
                    order_type.id = orders.id_order_type
                
                where
                    orders.id_client = companies.id
            )
            without triggers on order_type
        `,
            new Database([])
        );

        const result = builder.createTriggers();
        const orderTypeTrigger = result.find(item =>
            item.trigger.table.name === "order_type"
        );

        assert.ok(
            !orderTypeTrigger,

            "no triggers on order_type"
        );
    });


    it("no triggers for two tables", () => {
        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select
                    string_agg(distinct order_type.name, ', ') as orders_types_names,
                    string_agg(distinct country.name, ', ') as orders_countries_names

                from orders

                left join order_type on
                    order_type.id = orders.id_order_type
                
                left join countries as country on
                    country.id = orders.id_country

                where
                    orders.id_client = companies.id
            )
            without triggers on order_type
            without triggers on countries
        `,
            new Database([])
        );

        const result = builder.createTriggers();
        const orderTypeTrigger = result.find(item =>
            item.trigger.table.name === "order_type"
        );
        const countriesTrigger = result.find(item =>
            item.trigger.table.name === "countries"
        );

        assert.ok(
            !orderTypeTrigger,
            
            "no triggers on order_type"
        );
        assert.ok(
            !countriesTrigger,
            
            "no triggers on countries"
        );
    });

    it("error on unknown table", () => {

        const builder = new CacheTriggersBuilder(
            `cache totals for companies (
                select
                    string_agg(distinct country.name, ', ') as orders_countries_names

                from orders

                left join countries as country on
                    country.id = orders.id_country

                where
                    orders.id_client = companies.id
            )
            without triggers on country
        `,
            new Database([])
        );

        assert.throws(() => {
            builder.createTriggers();
        }, (err: Error) =>
            /unknown table to ignore triggers: public\.country/.test(err.message)
        );
    });
});