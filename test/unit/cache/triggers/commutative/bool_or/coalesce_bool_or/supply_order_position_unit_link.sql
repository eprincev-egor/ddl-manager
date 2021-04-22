create or replace function cache_is_3pl_shipped_for_supply_order_weight_position_on_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_position is not null then
            update supply_order_weight_position as position set
                is_3pl_shipped_bool_or_actual_netto_or_pcs = cm_array_remove_one_element(
                    is_3pl_shipped_bool_or_actual_netto_or_pcs,
                    old.actual_netto_or_pcs
                ),
                is_3pl_shipped_bool_or = (
                    select
                        bool_or(
                            item.actual_netto_or_pcs is not null
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            is_3pl_shipped_bool_or_actual_netto_or_pcs,
                            old.actual_netto_or_pcs
                        )
                    ) as item(actual_netto_or_pcs)
                ),
                is_3pl_shipped = coalesce(
                    ((
                        select
                            bool_or(
                                item.actual_netto_or_pcs is not null
                            )

                        from unnest(
                            cm_array_remove_one_element(
                                is_3pl_shipped_bool_or_actual_netto_or_pcs,
                                old.actual_netto_or_pcs
                            )
                        ) as item(actual_netto_or_pcs)
                    )),
                    false
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
                is_3pl_shipped_bool_or_actual_netto_or_pcs = array_append(
                    cm_array_remove_one_element(
                        is_3pl_shipped_bool_or_actual_netto_or_pcs,
                        old.actual_netto_or_pcs
                    ),
                    new.actual_netto_or_pcs
                ),
                is_3pl_shipped_bool_or = (
                    select
                        bool_or(
                            item.actual_netto_or_pcs is not null
                        )

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                is_3pl_shipped_bool_or_actual_netto_or_pcs,
                                old.actual_netto_or_pcs
                            ),
                            new.actual_netto_or_pcs
                        )
                    ) as item(actual_netto_or_pcs)
                ),
                is_3pl_shipped = coalesce(
                    ((
                        select
                            bool_or(
                                item.actual_netto_or_pcs is not null
                            )

                        from unnest(
                            array_append(
                                cm_array_remove_one_element(
                                    is_3pl_shipped_bool_or_actual_netto_or_pcs,
                                    old.actual_netto_or_pcs
                                ),
                                new.actual_netto_or_pcs
                            )
                        ) as item(actual_netto_or_pcs)
                    )),
                    false
                )
            where
                new.id_position = position.id;

            return new;
        end if;

        if old.id_position is not null then
            update supply_order_weight_position as position set
                is_3pl_shipped_bool_or_actual_netto_or_pcs = cm_array_remove_one_element(
                    is_3pl_shipped_bool_or_actual_netto_or_pcs,
                    old.actual_netto_or_pcs
                ),
                is_3pl_shipped_bool_or = (
                    select
                        bool_or(
                            item.actual_netto_or_pcs is not null
                        )

                    from unnest(
                        cm_array_remove_one_element(
                            is_3pl_shipped_bool_or_actual_netto_or_pcs,
                            old.actual_netto_or_pcs
                        )
                    ) as item(actual_netto_or_pcs)
                ),
                is_3pl_shipped = coalesce(
                    ((
                        select
                            bool_or(
                                item.actual_netto_or_pcs is not null
                            )

                        from unnest(
                            cm_array_remove_one_element(
                                is_3pl_shipped_bool_or_actual_netto_or_pcs,
                                old.actual_netto_or_pcs
                            )
                        ) as item(actual_netto_or_pcs)
                    )),
                    false
                )
            where
                old.id_position = position.id;
        end if;

        if new.id_position is not null then
            update supply_order_weight_position as position set
                is_3pl_shipped_bool_or_actual_netto_or_pcs = array_append(
                    is_3pl_shipped_bool_or_actual_netto_or_pcs,
                    new.actual_netto_or_pcs
                ),
                is_3pl_shipped_bool_or = is_3pl_shipped_bool_or
                or
                new.actual_netto_or_pcs is not null,
                is_3pl_shipped = coalesce(
                    (is_3pl_shipped_bool_or
                    or
                    new.actual_netto_or_pcs is not null),
                    false
                )
            where
                new.id_position = position.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_position is not null then
            update supply_order_weight_position as position set
                is_3pl_shipped_bool_or_actual_netto_or_pcs = array_append(
                    is_3pl_shipped_bool_or_actual_netto_or_pcs,
                    new.actual_netto_or_pcs
                ),
                is_3pl_shipped_bool_or = is_3pl_shipped_bool_or
                or
                new.actual_netto_or_pcs is not null,
                is_3pl_shipped = coalesce(
                    (is_3pl_shipped_bool_or
                    or
                    new.actual_netto_or_pcs is not null),
                    false
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