create or replace function cache_trains_ids_for_unit_on_train_unit_link()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                trains_ids_id_train = cm_array_remove_one_element(
                    trains_ids_id_train,
                    old.id_train
                ),
                trains_ids = (
                    select
                        array_agg(distinct item.id_train)

                    from unnest(
                        cm_array_remove_one_element(
                            trains_ids_id_train,
                            old.id_train
                        )
                    ) as item(id_train)
                )
            where
                old.id_unit = operation.unit.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_train is not distinct from old.id_train
            and
            new.id_unit is not distinct from old.id_unit
        then
            return new;
        end if;

        if
            new.id_unit is not distinct from old.id_unit
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_unit is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update operation.unit set
                trains_ids_id_train = array_append(
                    cm_array_remove_one_element(
                        trains_ids_id_train,
                        old.id_train
                    ),
                    new.id_train
                ),
                trains_ids = (
                    select
                        array_agg(distinct item.id_train)

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                trains_ids_id_train,
                                old.id_train
                            ),
                            new.id_train
                        )
                    ) as item(id_train)
                )
            where
                new.id_unit = operation.unit.id;

            return new;
        end if;

        if
            old.id_unit is not null
            and
            old.deleted = 0
        then
            update operation.unit set
                trains_ids_id_train = cm_array_remove_one_element(
                    trains_ids_id_train,
                    old.id_train
                ),
                trains_ids = (
                    select
                        array_agg(distinct item.id_train)

                    from unnest(
                        cm_array_remove_one_element(
                            trains_ids_id_train,
                            old.id_train
                        )
                    ) as item(id_train)
                )
            where
                old.id_unit = operation.unit.id;
        end if;

        if
            new.id_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                trains_ids_id_train = array_append(
                    trains_ids_id_train,
                    new.id_train
                ),
                trains_ids = case
                    when
                        array_position(trains_ids, new.id_train)
                        is null
                    then
                        array_append(trains_ids, new.id_train)
                    else
                        trains_ids
                end
            where
                new.id_unit = operation.unit.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_unit is not null
            and
            new.deleted = 0
        then
            update operation.unit set
                trains_ids_id_train = array_append(
                    trains_ids_id_train,
                    new.id_train
                ),
                trains_ids = case
                    when
                        array_position(trains_ids, new.id_train)
                        is null
                    then
                        array_append(trains_ids, new.id_train)
                    else
                        trains_ids
                end
            where
                new.id_unit = operation.unit.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_trains_ids_for_unit_on_train_unit_link
after insert or update of deleted, id_train, id_unit or delete
on public.train_unit_link
for each row
execute procedure cache_trains_ids_for_unit_on_train_unit_link();