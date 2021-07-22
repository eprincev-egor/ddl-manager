create or replace function cache_last_sea_for_units_on_operations()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then
        if
            old.units_ids is not null
            and
            old.type = 'sea'
            and
            old.deleted = 0
        then
            update units set
                (
                    __last_sea_id,
                    __last_sea_lvl,
                    last_sea_incoming_date,
                    last_sea_outgoing_date
                ) = (
                    select
                        last_sea.id as __last_sea_id,
                        last_sea.lvl as __last_sea_lvl,
                last_sea.incoming_date as last_sea_incoming_date,
                last_sea.outgoing_date as last_sea_outgoing_date

                    from operations as last_sea

                    where
                        last_sea.units_ids && ARRAY[units.id] :: bigint[]
                and
                last_sea.type = 'sea'
                and
                last_sea.deleted = 0
            order by
                last_sea.lvl desc nulls first,
                last_sea.id desc nulls first
            limit 1
                )
            where
                units.id = any( old.units_ids )
                and
                units.__last_sea_id = old.id;

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.incoming_date is not distinct from old.incoming_date
            and
            new.lvl is not distinct from old.lvl
            and
            new.outgoing_date is not distinct from old.outgoing_date
            and
            new.type is not distinct from old.type
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        matched_old = coalesce(old.type = 'sea'
            and
            old.deleted = 0, false);
        matched_new = coalesce(new.type = 'sea'
            and
            new.deleted = 0, false);

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
            inserted_units_ids = null;
            not_changed_units_ids = null;
            deleted_units_ids = old.units_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_units_ids = new.units_ids;
            not_changed_units_ids = null;
            deleted_units_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
            not_changed_units_ids = cm_get_not_changed_elements(old.units_ids, new.units_ids);
            deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);
        end if;


        if not_changed_units_ids is not null then
            if new.lvl is not distinct from old.lvl then
                if
                    new.incoming_date is distinct from old.incoming_date
                    or
                    new.outgoing_date is distinct from old.outgoing_date
                then
                    update units set
                        __last_sea_id = new.id,
                        __last_sea_lvl = new.lvl,
                        last_sea_incoming_date = new.incoming_date,
                        last_sea_outgoing_date = new.outgoing_date
                    where
                        units.id = any( not_changed_units_ids )
                        and
                        units.__last_sea_id = new.id
                        and
                        (
                            units.last_sea_incoming_date is distinct from new.incoming_date
                            or
                            units.last_sea_outgoing_date is distinct from new.outgoing_date
                        );
                end if;
            else
                if
                    new.lvl is not distinct from old.lvl
                    and
                    new.id > old.id
                    or
                    new.lvl is null
                    and
                    old.lvl is not null
                    or
                    new.lvl > old.lvl
                then
                    update units set
                        __last_sea_id = new.id,
                        __last_sea_lvl = new.lvl,
                        last_sea_incoming_date = new.incoming_date,
                        last_sea_outgoing_date = new.outgoing_date
                    where
                        units.id = any( not_changed_units_ids )
                        and
                        (
                            units.__last_sea_id = new.id
                            or
                            units.__last_sea_id is null
                            or
                            units.__last_sea_lvl is not distinct from new.lvl
                            and
                            units.__last_sea_id < new.id
                            or
                            units.__last_sea_lvl is not null
                            and
                            new.lvl is null
                            or
                            units.__last_sea_lvl < new.lvl
                        );
                else
                    update units set
                        (
                            __last_sea_id,
                            __last_sea_lvl,
                            last_sea_incoming_date,
                            last_sea_outgoing_date
                        ) = (
                            select
                                last_sea.id as __last_sea_id,
                                last_sea.lvl as __last_sea_lvl,
                last_sea.incoming_date as last_sea_incoming_date,
                last_sea.outgoing_date as last_sea_outgoing_date

                            from operations as last_sea

                            where
                                last_sea.units_ids && ARRAY[units.id] :: bigint[]
                and
                last_sea.type = 'sea'
                and
                last_sea.deleted = 0
            order by
                last_sea.lvl desc nulls first,
                last_sea.id desc nulls first
            limit 1
                        )
                    where
                        units.id = any( not_changed_units_ids );
                end if;
            end if;
        end if;

        if deleted_units_ids is not null then
            update units set
                (
                    __last_sea_id,
                    __last_sea_lvl,
                    last_sea_incoming_date,
                    last_sea_outgoing_date
                ) = (
                    select
                        last_sea.id as __last_sea_id,
                        last_sea.lvl as __last_sea_lvl,
                last_sea.incoming_date as last_sea_incoming_date,
                last_sea.outgoing_date as last_sea_outgoing_date

                    from operations as last_sea

                    where
                        last_sea.units_ids && ARRAY[units.id] :: bigint[]
                and
                last_sea.type = 'sea'
                and
                last_sea.deleted = 0
            order by
                last_sea.lvl desc nulls first,
                last_sea.id desc nulls first
            limit 1
                )
            where
                units.id = any( deleted_units_ids )
                and
                units.__last_sea_id = new.id;
        end if;

        if inserted_units_ids is not null then
            update units set
                __last_sea_id = new.id,
                __last_sea_lvl = new.lvl,
                last_sea_incoming_date = new.incoming_date,
                last_sea_outgoing_date = new.outgoing_date
            where
                units.id = any( inserted_units_ids )
                and
                (
                    units.__last_sea_id is null
                    or
                    units.__last_sea_lvl is not distinct from new.lvl
                    and
                    units.__last_sea_id < new.id
                    or
                    units.__last_sea_lvl is not null
                    and
                    new.lvl is null
                    or
                    units.__last_sea_lvl < new.lvl
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.units_ids is not null
            and
            new.type = 'sea'
            and
            new.deleted = 0
        then
            update units set
                __last_sea_id = new.id,
                __last_sea_lvl = new.lvl,
                last_sea_incoming_date = new.incoming_date,
                last_sea_outgoing_date = new.outgoing_date
            where
                units.id = any( new.units_ids )
                and
                (
                    units.__last_sea_id is null
                    or
                    units.__last_sea_lvl is not distinct from new.lvl
                    and
                    units.__last_sea_id < new.id
                    or
                    units.__last_sea_lvl is not null
                    and
                    new.lvl is null
                    or
                    units.__last_sea_lvl < new.lvl
                );

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_sea_for_units_on_operations
after insert or update of deleted, incoming_date, lvl, outgoing_date, type, units_ids or delete
on public.operations
for each row
execute procedure cache_last_sea_for_units_on_operations();