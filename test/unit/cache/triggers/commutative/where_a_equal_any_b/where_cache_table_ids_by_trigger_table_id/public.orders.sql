create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.deleted = 0 then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_numbers
                ) = (
                    select
                            string_agg(distinct 
                                source_row.doc_number,
                                ', '
                                                        ) as orders_numbers
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
                        source_row.id = any(companies.order_ids)
                        and
                        source_row.deleted = 0
                )
            where
                companies.order_ids && cm_build_array_for((null::public.companies).order_ids, old.id);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.doc_number is not distinct from old.doc_number
        then
            return new;
        end if;

        if new.deleted is not distinct from old.deleted then
            if not coalesce(new.deleted = 0, false) then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
            ),
            TG_OP
        ),
                (
                    orders_numbers
                ) = (
                    select
                            string_agg(distinct 
                                source_row.doc_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id = any(companies.order_ids)
                        and
                        source_row.deleted = 0
                )
            where
                companies.order_ids && cm_build_array_for((null::public.companies).order_ids, new.id);

            return new;
        end if;

        if old.deleted = 0 then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_numbers
                ) = (
                    select
                            string_agg(distinct 
                                source_row.doc_number,
                                ', '
                                                        ) as orders_numbers
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
                        source_row.id = any(companies.order_ids)
                        and
                        source_row.deleted = 0
                )
            where
                companies.order_ids && cm_build_array_for((null::public.companies).order_ids, old.id);
        end if;

        if new.deleted = 0 then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
            ),
            TG_OP
        ),
                (
                    orders_numbers
                ) = (
                    select
                            string_agg(distinct 
                                source_row.doc_number,
                                ', '
                                                        ) as orders_numbers
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id = any(companies.order_ids)
                        and
                        source_row.deleted = 0
                )
            where
                companies.order_ids && cm_build_array_for((null::public.companies).order_ids, new.id);
        end if;

        return new;
    end if;
end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after update of deleted, doc_number or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();