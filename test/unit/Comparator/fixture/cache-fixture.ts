import { Column } from "../../../../lib/database/schema/Column";
import { DatabaseFunction } from "../../../../lib/database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../../lib/database/schema/DatabaseTrigger";
import { Table } from "../../../../lib/database/schema/Table";
import { TableID } from "../../../../lib/database/schema/TableID";
import { Comment } from "../../../../lib/database/schema/Comment";
import { FileParser } from "../../../../lib/parser";

export const someFileParams = {
    name: "some_file.sql",
    folder: "/",
    path: "/some_file.sql"
};

export const testCache = FileParser.parseCache(`
    cache totals for companies (
        select
            sum(orders.profit) as orders_profit
        from orders
        where
            orders.id_client = companies.id
    )
`);

export const testFileWithCache = {
    ...someFileParams,
    content: {
        cache: [testCache]
    }
};

export const testCacheWithOtherName = FileParser.parseCache(`
    cache another_name for companies (
        select
            sum(orders.profit) as orders_profit
        from orders
        where
            orders.id_client = companies.id
    )
`);

export const testFileWithCacheWithOtherName = {
    ...someFileParams,
    content: {
        cache: [testCacheWithOtherName]
    }
};

export const testCacheWithOtherCalc = FileParser.parseCache(`
    cache totals for companies (
        select
            sum( orders.profit * 2 ) as orders_profit
        from orders
        where
            orders.id_client = companies.id
    )
`);

export const testFileWithCacheWithOtherCalc = {
    ...someFileParams,
    content: {
        cache: [testCacheWithOtherCalc]
    }
};

export const testCacheWithOtherColumnType = FileParser.parseCache(`
    cache totals for companies (
        select
            array_agg( orders.profit ) as orders_profit
        from orders
        where
            orders.id_client = companies.id
    )
`);

export const testFileWithCacheWithOtherColumnType = {
    ...someFileParams,
    content: {
        cache: [testCacheWithOtherColumnType]
    }
};

export const someCacheFuncParams = {
    ...FileParser.parseFunction(`
create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_profit = orders_profit - coalesce(old.profit, 0)
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                orders_profit = orders_profit - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_profit = orders_profit - coalesce(old.profit, 0)
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_profit = orders_profit + coalesce(new.profit, 0)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_profit = orders_profit + coalesce(new.profit, 0)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;
    `),
    comment: Comment.fromFs({
        objectType: "trigger",
        cacheSignature: "cache totals for companies"
    })
};

export const someCacheTriggerParams = {
    table: new TableID(
        "public",
        "orders"
    ),
    after: true,
    insert: true,
    update: true,
    updateOf: ["id_client", "profit"],
    delete: true,
    name: "cache_totals_for_companies_on_orders",
    procedure: {
        schema: "public",
        name: "cache_totals_for_companies_on_orders",
        args: []
    },
    comment: Comment.fromFs({
        objectType: "trigger",
        cacheSignature: "cache totals for companies"
    })
};

export const testCacheFunc = new DatabaseFunction({
    ...someCacheFuncParams
});

export const testCacheTrigger = new DatabaseTrigger({
    ...someCacheTriggerParams
});

export const companiesId = new TableID("public", "companies");
export const testCacheColumn = new Column(
    companiesId,
    "orders_profit",
    "numeric",
    "0",
    Comment.fromFs({
        objectType: "column",
        cacheSignature: "cache totals for companies",
        cacheSelect: `
select
    coalesce(sum(orders.profit), 0) as orders_profit
from orders
where
    orders.id_client = companies.id`.trim()
    })
);

export const testTableWithCache = new Table(
    "public", "companies",
    [
        new Column(
            companiesId,
            "id",
            "integer"
        ),
        testCacheColumn
    ]
);


export const ordersId = new TableID("public", "orders");
export const testTableSource = new Table(
    "public", "orders",
    [
        new Column(
            ordersId,
            "id",
            "integer"
        ),
        new Column(
            ordersId,
            "profit",
            "integer"
        )
    ]
);