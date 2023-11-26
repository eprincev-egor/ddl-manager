create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update companies set
            __totals_json__ = __totals_json__ - old.id::text,
            (
                orders_total
            ) = (
                select
                        sum(source_row.id) as orders_total
                from (
                    select
                            record.*
                    from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                    left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                        true
                ) as source_row
                where
                    source_row.id = any(companies.bigint_orders_ids)
            )
        where
            companies.bigint_orders_ids && cm_build_array_for((null::public.companies).bigint_orders_ids, old.id);

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