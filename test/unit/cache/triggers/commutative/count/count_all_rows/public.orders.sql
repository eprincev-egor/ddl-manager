create or replace function cache_totals_for_some_report_row_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update some_report_row set
            __totals_json__ = __totals_json__ - old.id::text,
            (
                orders_count
            ) = (
                select
                        count(*) as orders_count
                from (
                    select
                            record.*
                    from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                    left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                        true
                ) as source_row
            );

        return old;
    end if;


    if TG_OP = 'INSERT' then

        update some_report_row set
            __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id
            ),
            TG_OP
        ),
            (
                orders_count
            ) = (
                select
                        count(*) as orders_count
                from (
                    select
                            record.*
                    from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id
                ),
                TG_OP
            )
) as json_entry

                    left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                        true
                ) as source_row
            );

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_some_report_row_on_orders
after insert or delete
on public.orders
for each row
execute procedure cache_totals_for_some_report_row_on_orders();