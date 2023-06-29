create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_country is not null then
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
                        source_row.id_country = companies.id_country
                )
            where
                old.id_country = companies.id_country;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.doc_number is not distinct from old.doc_number
            and
            new.id_country is not distinct from old.id_country
        then
            return new;
        end if;

        if new.id_country is not distinct from old.id_country then
            if new.id_country is null then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
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
                null::jsonb,
                jsonb_build_object(
                    'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_country = companies.id_country
                )
            where
                new.id_country = companies.id_country;

            return new;
        end if;

        if old.id_country is not null then
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
                        source_row.id_country = companies.id_country
                )
            where
                old.id_country = companies.id_country;
        end if;

        if new.id_country is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
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
                null::jsonb,
                jsonb_build_object(
                    'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_country = companies.id_country
                )
            where
                new.id_country = companies.id_country;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_country is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
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
                null::jsonb,
                jsonb_build_object(
                    'doc_number', new.doc_number,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_country = companies.id_country
                )
            where
                new.id_country = companies.id_country;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of doc_number, id_country or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();