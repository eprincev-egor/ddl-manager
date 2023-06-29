create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            (
                old.id_client is not null
                or
                old.id_partner is not null
            )
            and
            old.deleted = 0
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_total
                ) = (
                    select
                            sum(source_row.profit) as orders_total
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
                        companies.id in (source_row.id_client, source_row.id_partner)
                        and
                        source_row.deleted = 0
                )
            where
                companies.id in (old.id_client, old.id_partner);
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_client is not distinct from old.id_client
            and
            new.id_partner is not distinct from old.id_partner
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if
            new.id_client is not distinct from old.id_client
            and
            new.id_partner is not distinct from old.id_partner
            and
            new.deleted is not distinct from old.deleted
        then
            if
                (
                    new.id_client is null
                    or
                    new.id_partner is null
                )
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(source_row.profit) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        companies.id in (source_row.id_client, source_row.id_partner)
                        and
                        source_row.deleted = 0
                )
            where
                companies.id in (new.id_client, new.id_partner);

            return new;
        end if;

        if
            (
                old.id_client is not null
                or
                old.id_partner is not null
            )
            and
            old.deleted = 0
        then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_total
                ) = (
                    select
                            sum(source_row.profit) as orders_total
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
                        companies.id in (source_row.id_client, source_row.id_partner)
                        and
                        source_row.deleted = 0
                )
            where
                companies.id in (old.id_client, old.id_partner);
        end if;

        if
            (
                new.id_client is not null
                or
                new.id_partner is not null
            )
            and
            new.deleted = 0
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(source_row.profit) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        companies.id in (source_row.id_client, source_row.id_partner)
                        and
                        source_row.deleted = 0
                )
            where
                companies.id in (new.id_client, new.id_partner);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            (
                new.id_client is not null
                or
                new.id_partner is not null
            )
            and
            new.deleted = 0
        then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(source_row.profit) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'deleted', new.deleted,'id', new.id,'id_client', new.id_client,'id_partner', new.id_partner,'profit', new.profit
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        companies.id in (source_row.id_client, source_row.id_partner)
                        and
                        source_row.deleted = 0
                )
            where
                companies.id in (new.id_client, new.id_partner);
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of deleted, id_client, id_partner, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();