create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_companies_ids integer[];
declare not_changed_companies_ids integer[];
declare deleted_companies_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.companies_ids is not null
            and
            old.deleted = 0
        then
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
                        source_row.companies_ids && ARRAY[companies.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( old.companies_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.companies_ids, old.companies_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            new.doc_number is not distinct from old.doc_number
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
            inserted_companies_ids = null;
            not_changed_companies_ids = null;
            deleted_companies_ids = old.companies_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_companies_ids = new.companies_ids;
            not_changed_companies_ids = null;
            deleted_companies_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_companies_ids = cm_get_inserted_elements(old.companies_ids, new.companies_ids);
            not_changed_companies_ids = cm_get_not_changed_elements(old.companies_ids, new.companies_ids);
            deleted_companies_ids = cm_get_deleted_elements(old.companies_ids, new.companies_ids);
        end if;

        if not_changed_companies_ids is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
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
                    'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.companies_ids && ARRAY[companies.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( not_changed_companies_ids::bigint[] );
        end if;

        if deleted_companies_ids is not null then
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
                        source_row.companies_ids && ARRAY[companies.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( deleted_companies_ids::bigint[] );
        end if;

        if inserted_companies_ids is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
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
                    'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.companies_ids && ARRAY[companies.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( inserted_companies_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.companies_ids is not null
            and
            new.deleted = 0
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
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
                    'companies_ids', new.companies_ids,'deleted', new.deleted,'doc_number', new.doc_number,'id', new.id
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.companies_ids && ARRAY[companies.id] :: bigint[]
                        and
                        source_row.deleted = 0
                )
            where
                companies.id = any( new.companies_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of companies_ids, deleted, doc_number or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();