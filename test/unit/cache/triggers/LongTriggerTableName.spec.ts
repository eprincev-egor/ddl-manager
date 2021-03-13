import { CacheTriggersBuilder } from "../../../../lib/cache/CacheTriggersBuilder";
import { Database } from "../../../../lib/database/schema/Database";
import assert from "assert";

describe("try use alias for name if default is long", () => {

    it("try use alias for name if default is long", () => {
        const builder = new CacheTriggersBuilder(
            `cache test_my_very_very_long_cache_name for some_table (
                select
                    sum(link.profit) as total_profit

                from my_very_very_long_table_name as link
                where
                    link.id_client = some_table.id
            )
        `,
            new Database([])
        );

        const result = builder.createTriggers();
        const cacheTrigger = result.find(item =>
            item.trigger.table.name === "my_very_very_long_table_name"
        )!;

        assert.strictEqual(
            cacheTrigger.name,
            "cache_test_my_very_very_long_cache_name_for_some_table_on_link"
        );
    });

});