create or replace function cache_trains_for_unit_on_train()
returns trigger as $body$
declare inserted_units_ids int8[];
declare deleted_units_ids int8[];
begin

    if TG_OP = 'DELETE' then

        if
            old.units_ids is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                train_numbers_number = cm_array_remove_one_element(
                    train_numbers_number,
                    old.number
                ),
                train_numbers = (
                    select
                        string_agg(distinct item.number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            train_numbers_number,
                            old.number
                        )
                    ) as item(number)
                )
            where
                operation.unit.id = any( old.units_ids::bigint[] );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.number is not distinct from old.number
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
        deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);

        if
            cm_equal_arrays(new.units_ids, old.units_ids)
            and
            new.deleted is not distinct from old.deleted
        then
            if not coalesce(new.deleted = 0, false) then
                return new;
            end if;

            update operation.unit set
                train_numbers_number = array_append(
                    cm_array_remove_one_element(
                        train_numbers_number,
                        old.number
                    ),
                    new.number
                ),
                train_numbers = (
                    select
                        string_agg(distinct item.number, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                train_numbers_number,
                                old.number
                            ),
                            new.number
                        )
                    ) as item(number)
                )
            where
                operation.unit.id = any( new.units_ids::bigint[] );

            return new;
        end if;

        if
            deleted_units_ids is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                train_numbers_number = cm_array_remove_one_element(
                    train_numbers_number,
                    old.number
                ),
                train_numbers = (
                    select
                        string_agg(distinct item.number, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            train_numbers_number,
                            old.number
                        )
                    ) as item(number)
                )
            where
                operation.unit.id = any( deleted_units_ids::bigint[] );
        end if;

        if
            inserted_units_ids is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                train_numbers_number = array_append(
                    train_numbers_number,
                    new.number
                ),
                train_numbers = case
                    when
                        array_position(
                            train_numbers_number,
                            new.number
                        )
                        is null
                    then
                        coalesce(
                            train_numbers ||
                            coalesce(', '
|| new.number, ''),
                            new.number
                        )
                    else
                        train_numbers
                end
            where
                operation.unit.id = any( inserted_units_ids::bigint[] );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.units_ids is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                train_numbers_number = array_append(
                    train_numbers_number,
                    new.number
                ),
                train_numbers = case
                    when
                        array_position(
                            train_numbers_number,
                            new.number
                        )
                        is null
                    then
                        coalesce(
                            train_numbers ||
                            coalesce(', '
|| new.number, ''),
                            new.number
                        )
                    else
                        train_numbers
                end
            where
                operation.unit.id = any( new.units_ids::bigint[] );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_trains_for_unit_on_train
after insert or update of deleted, number, units_ids or delete
on public.train
for each row
execute procedure cache_trains_for_unit_on_train();