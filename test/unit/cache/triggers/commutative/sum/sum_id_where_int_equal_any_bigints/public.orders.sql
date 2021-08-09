create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update companies set
            orders_total = coalesce(orders_total, 0) - coalesce(old.id, 0)
        where
            companies.bigint_orders_ids && ARRAY[ old.id ]::bigint[];

        return old;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();