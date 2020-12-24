create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update companies set
            orders_total = orders_total - coalesce(old.id, 0)
        where
            companies.bigint_orders_ids && ARRAY[ old.id ]::bigint[];

        return old;
    end if;


    if TG_OP = 'INSERT' then

        update companies set
            orders_total = orders_total + coalesce(new.id, 0)
        where
            companies.bigint_orders_ids && ARRAY[ new.id ]::bigint[];

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();