create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_profit_sum_sales = orders_profit_sum_sales - coalesce(old.sales, 0),
                orders_profit_sum_buys = orders_profit_sum_buys - coalesce(old.buys, 0),
                orders_profit = (orders_profit_sum_sales - coalesce(old.sales, 0)) - (orders_profit_sum_buys - coalesce(old.buys, 0))
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.buys is not distinct from old.buys
            and
            new.id_client is not distinct from old.id_client
            and
            new.sales is not distinct from old.sales
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_profit_sum_sales = orders_profit_sum_sales - coalesce(old.sales, 0) + coalesce(new.sales, 0),
                orders_profit_sum_buys = orders_profit_sum_buys - coalesce(old.buys, 0) + coalesce(new.buys, 0),
                orders_profit = (orders_profit_sum_sales - coalesce(old.sales, 0) + coalesce(new.sales, 0)) - (orders_profit_sum_buys - coalesce(old.buys, 0) + coalesce(new.buys, 0))
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_profit_sum_sales = orders_profit_sum_sales - coalesce(old.sales, 0),
                orders_profit_sum_buys = orders_profit_sum_buys - coalesce(old.buys, 0),
                orders_profit = (orders_profit_sum_sales - coalesce(old.sales, 0)) - (orders_profit_sum_buys - coalesce(old.buys, 0))
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_profit_sum_sales = orders_profit_sum_sales + coalesce(new.sales, 0),
                orders_profit_sum_buys = orders_profit_sum_buys + coalesce(new.buys, 0),
                orders_profit = (orders_profit_sum_sales + coalesce(new.sales, 0)) - (orders_profit_sum_buys + coalesce(new.buys, 0))
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_profit_sum_sales = orders_profit_sum_sales + coalesce(new.sales, 0),
                orders_profit_sum_buys = orders_profit_sum_buys + coalesce(new.buys, 0),
                orders_profit = (orders_profit_sum_sales + coalesce(new.sales, 0)) - (orders_profit_sum_buys + coalesce(new.buys, 0))
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of buys, id_client, sales or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();