create or replace function cache_totals_for_companies_on_companies()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_parent_company is not null then
            update companies as parent_company set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    children_companies_ids
                ) = (
                    select
                            array_agg(source_row.id) as children_companies_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.companies, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_parent_company = parent_company.id
                )
            where
                old.id_parent_company = parent_company.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.id_parent_company is not distinct from old.id_parent_company then
            return new;
        end if;

        if new.id_parent_company is not distinct from old.id_parent_company then
            if new.id_parent_company is null then
                return new;
            end if;

            update companies as parent_company set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_parent_company', new.id_parent_company
            ),
            TG_OP
        ),
                (
                    children_companies_ids
                ) = (
                    select
                            array_agg(source_row.id) as children_companies_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_parent_company', new.id_parent_company
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.companies, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_parent_company = parent_company.id
                )
            where
                new.id_parent_company = parent_company.id;

            return new;
        end if;

        if old.id_parent_company is not null then
            update companies as parent_company set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    children_companies_ids
                ) = (
                    select
                            array_agg(source_row.id) as children_companies_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.companies, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_parent_company = parent_company.id
                )
            where
                old.id_parent_company = parent_company.id;
        end if;

        if new.id_parent_company is not null then
            update companies as parent_company set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_parent_company', new.id_parent_company
            ),
            TG_OP
        ),
                (
                    children_companies_ids
                ) = (
                    select
                            array_agg(source_row.id) as children_companies_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_parent_company', new.id_parent_company
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.companies, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_parent_company = parent_company.id
                )
            where
                new.id_parent_company = parent_company.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_parent_company is not null then
            update companies as parent_company set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'id', new.id,'id_parent_company', new.id_parent_company
            ),
            TG_OP
        ),
                (
                    children_companies_ids
                ) = (
                    select
                            array_agg(source_row.id) as children_companies_ids
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'id', new.id,'id_parent_company', new.id_parent_company
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.companies, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_parent_company = parent_company.id
                )
            where
                new.id_parent_company = parent_company.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_companies
after insert or update of id_parent_company or delete
on public.companies
for each row
execute procedure cache_totals_for_companies_on_companies();