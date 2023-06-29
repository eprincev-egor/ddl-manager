create or replace function cache_is_3pl_shipped_for_supply_order_weight_position_on_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_position is not null then
            update supply_order_weight_position as position set
                __is_3pl_shipped_json__ = __is_3pl_shipped_json__ - old.id::text,
                (
                    is_3pl_shipped
                ) = (
                    select
                            coalesce(
                                bool_or(
                                    source_row.actual_netto_or_pcs is not null
                                    ),
                                false
                                                        ) as is_3pl_shipped
                    from (
                        select
                                record.*
                        from jsonb_each(
    __is_3pl_shipped_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.supply_order_position_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_position = position.id
                )
            where
                old.id_position = position.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.actual_netto_or_pcs is not distinct from old.actual_netto_or_pcs
            and
            new.id_position is not distinct from old.id_position
        then
            return new;
        end if;

        if new.id_position is not distinct from old.id_position then
            if new.id_position is null then
                return new;
            end if;

            update supply_order_weight_position as position set
                __is_3pl_shipped_json__ = cm_merge_json(
            __is_3pl_shipped_json__,
            null::jsonb,
            jsonb_build_object(
                'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
            ),
            TG_OP
        ),
                (
                    is_3pl_shipped
                ) = (
                    select
                            coalesce(
                                bool_or(
                                    source_row.actual_netto_or_pcs is not null
                                    ),
                                false
                                                        ) as is_3pl_shipped
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __is_3pl_shipped_json__,
                null::jsonb,
                jsonb_build_object(
                    'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.supply_order_position_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_position = position.id
                )
            where
                new.id_position = position.id;

            return new;
        end if;

        if old.id_position is not null then
            update supply_order_weight_position as position set
                __is_3pl_shipped_json__ = __is_3pl_shipped_json__ - old.id::text,
                (
                    is_3pl_shipped
                ) = (
                    select
                            coalesce(
                                bool_or(
                                    source_row.actual_netto_or_pcs is not null
                                    ),
                                false
                                                        ) as is_3pl_shipped
                    from (
                        select
                                record.*
                        from jsonb_each(
    __is_3pl_shipped_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::public.supply_order_position_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_position = position.id
                )
            where
                old.id_position = position.id;
        end if;

        if new.id_position is not null then
            update supply_order_weight_position as position set
                __is_3pl_shipped_json__ = cm_merge_json(
            __is_3pl_shipped_json__,
            null::jsonb,
            jsonb_build_object(
                'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
            ),
            TG_OP
        ),
                (
                    is_3pl_shipped
                ) = (
                    select
                            coalesce(
                                bool_or(
                                    source_row.actual_netto_or_pcs is not null
                                    ),
                                false
                                                        ) as is_3pl_shipped
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __is_3pl_shipped_json__,
                null::jsonb,
                jsonb_build_object(
                    'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.supply_order_position_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_position = position.id
                )
            where
                new.id_position = position.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_position is not null then
            update supply_order_weight_position as position set
                __is_3pl_shipped_json__ = cm_merge_json(
            __is_3pl_shipped_json__,
            null::jsonb,
            jsonb_build_object(
                'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
            ),
            TG_OP
        ),
                (
                    is_3pl_shipped
                ) = (
                    select
                            coalesce(
                                bool_or(
                                    source_row.actual_netto_or_pcs is not null
                                    ),
                                false
                                                        ) as is_3pl_shipped
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __is_3pl_shipped_json__,
                null::jsonb,
                jsonb_build_object(
                    'actual_netto_or_pcs', new.actual_netto_or_pcs,'id', new.id,'id_position', new.id_position
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::public.supply_order_position_unit_link, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.id_position = position.id
                )
            where
                new.id_position = position.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_is_3pl_shipped_for_supply_order_weight_position_on_link
after insert or update of actual_netto_or_pcs, id_position or delete
on public.supply_order_position_unit_link
for each row
execute procedure cache_is_3pl_shipped_for_supply_order_weight_position_on_link();