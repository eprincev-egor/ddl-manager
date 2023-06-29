create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    from_countries,
                    to_countries
                ) = (
                    select
                            string_agg(country_from.name, ', ') as from_countries,
                            string_agg(country_to.name, ', ') as to_countries
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country_from on
                        country_from.id = source_row.id_country_from

                    left join countries as country_to on
                        source_row.id_country_to = country_to.id
                    where
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.id_country_from is not distinct from old.id_country_from
            and
            new.id_country_to is not distinct from old.id_country_to
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
            ),
            TG_OP
        ),
                (
                    from_countries,
                    to_countries
                ) = (
                    select
                            string_agg(country_from.name, ', ') as from_countries,
                            string_agg(country_to.name, ', ') as to_countries
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country_from on
                        country_from.id = source_row.id_country_from

                    left join countries as country_to on
                        source_row.id_country_to = country_to.id
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    from_countries,
                    to_countries
                ) = (
                    select
                            string_agg(country_from.name, ', ') as from_countries,
                            string_agg(country_to.name, ', ') as to_countries
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country_from on
                        country_from.id = source_row.id_country_from

                    left join countries as country_to on
                        source_row.id_country_to = country_to.id
                    where
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
            ),
            TG_OP
        ),
                (
                    from_countries,
                    to_countries
                ) = (
                    select
                            string_agg(country_from.name, ', ') as from_countries,
                            string_agg(country_to.name, ', ') as to_countries
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country_from on
                        country_from.id = source_row.id_country_from

                    left join countries as country_to on
                        source_row.id_country_to = country_to.id
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
            ),
            TG_OP
        ),
                (
                    from_countries,
                    to_countries
                ) = (
                    select
                            string_agg(country_from.name, ', ') as from_countries,
                            string_agg(country_to.name, ', ') as to_countries
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_client', new.id_client,'id_country_from', new.id_country_from,'id_country_to', new.id_country_to
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country_from on
                        country_from.id = source_row.id_country_from

                    left join countries as country_to on
                        source_row.id_country_to = country_to.id
                    where
                        source_row.id_client = companies.id
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, id_country_from, id_country_to or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();