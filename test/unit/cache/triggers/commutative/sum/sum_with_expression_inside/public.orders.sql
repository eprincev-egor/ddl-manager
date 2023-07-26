create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    orders_total
                ) = (
                    select
                            sum(
                                (source_row.debet - source_row.credit)
                                * source_row.quantity
                                                        ) as orders_total
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
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.credit is not distinct from old.credit
            and
            new.debet is not distinct from old.debet
            and
            new.id_client is not distinct from old.id_client
            and
            new.quantity is not distinct from old.quantity
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
            jsonb_build_object(
                'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(
                                (source_row.debet - source_row.credit)
                                * source_row.quantity
                                                        ) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
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
                    orders_total
                ) = (
                    select
                            sum(
                                (source_row.debet - source_row.credit)
                                * source_row.quantity
                                                        ) as orders_total
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
                        source_row.id_client = companies.id
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            jsonb_build_object(
                'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(
                                (source_row.debet - source_row.credit)
                                * source_row.quantity
                                                        ) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
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
            jsonb_build_object(
                'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
            ),
            TG_OP
        ),
                (
                    orders_total
                ) = (
                    select
                            sum(
                                (source_row.debet - source_row.credit)
                                * source_row.quantity
                                                        ) as orders_total
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                jsonb_build_object(
                    'credit', new.credit,'debet', new.debet,'id', new.id,'id_client', new.id_client,'quantity', new.quantity
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.orders, json_entry.value) as record on
                            true
                    ) as source_row
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
after insert or update of credit, debet, id_client, quantity or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();