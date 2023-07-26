create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_clients_ids integer[];
declare not_changed_clients_ids integer[];
declare deleted_clients_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.clients_ids is not null
            and
            old.deleted = 0
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    countries_names
                ) = (
                    select
                            string_agg(country.name, ', ') as countries_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country on
                        country.id = source_row.id_country
                    where
                        source_row.clients_ids && ARRAY[companies.id]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( old.clients_ids );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.clients_ids, old.clients_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            new.id_country is not distinct from old.id_country
        then
            return new;
        end if;

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_clients_ids = null;
            not_changed_clients_ids = null;
            deleted_clients_ids = old.clients_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_clients_ids = new.clients_ids;
            not_changed_clients_ids = null;
            deleted_clients_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_clients_ids = cm_get_inserted_elements(old.clients_ids, new.clients_ids);
            not_changed_clients_ids = cm_get_not_changed_elements(old.clients_ids, new.clients_ids);
            deleted_clients_ids = cm_get_deleted_elements(old.clients_ids, new.clients_ids);
        end if;

        if not_changed_clients_ids is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
            ),
            TG_OP
        ),
                (
                    countries_names
                ) = (
                    select
                            string_agg(country.name, ', ') as countries_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country on
                        country.id = source_row.id_country
                    where
                        source_row.clients_ids && ARRAY[companies.id]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( not_changed_clients_ids );
        end if;

        if deleted_clients_ids is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    countries_names
                ) = (
                    select
                            string_agg(country.name, ', ') as countries_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country on
                        country.id = source_row.id_country
                    where
                        source_row.clients_ids && ARRAY[companies.id]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( deleted_clients_ids );
        end if;

        if inserted_clients_ids is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
            ),
            TG_OP
        ),
                (
                    countries_names
                ) = (
                    select
                            string_agg(country.name, ', ') as countries_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country on
                        country.id = source_row.id_country
                    where
                        source_row.clients_ids && ARRAY[companies.id]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( inserted_clients_ids );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.clients_ids is not null
            and
            new.deleted = 0
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
            ),
            TG_OP
        ),
                (
                    countries_names
                ) = (
                    select
                            string_agg(country.name, ', ') as countries_names
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'clients_ids', new.clients_ids,'deleted', new.deleted,'id', new.id,'id_country', new.id_country
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row

                    left join countries as country on
                        country.id = source_row.id_country
                    where
                        source_row.clients_ids && ARRAY[companies.id]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( new.clients_ids );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of clients_ids, deleted, id_country or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();