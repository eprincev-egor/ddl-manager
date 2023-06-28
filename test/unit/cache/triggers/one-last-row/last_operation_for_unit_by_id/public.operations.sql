create or replace function cache_last_operation_for_units_on_operations()
returns trigger as $body$
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then
        if old.units_ids is not null then
            update units set
                (
                    __last_operation_id,
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            last_operation.id as __last_operation_id,
                            last_operation.incoming_date as last_operation_incoming_date,
                            last_operation.outgoing_date as last_operation_outgoing_date
                    from operations as last_operation
                    where
                        last_operation.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        last_operation.id desc nulls first
                    limit 1
                )
            where
                units.id = any( old.units_ids )
                and
                units.__last_operation_id = old.id;

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.incoming_date is not distinct from old.incoming_date
            and
            new.outgoing_date is not distinct from old.outgoing_date
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
        not_changed_units_ids = cm_get_not_changed_elements(old.units_ids, new.units_ids);
        deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);


        if not_changed_units_ids is not null then
            if
                new.incoming_date is distinct from old.incoming_date
                or
                new.outgoing_date is distinct from old.outgoing_date
            then
                update units set
                    __last_operation_id = new.id,
                    last_operation_incoming_date = new.incoming_date,
                    last_operation_outgoing_date = new.outgoing_date
                where
                    units.id = any( not_changed_units_ids )
                    and
                    units.__last_operation_id = new.id
                    and
                    (
                        units.last_operation_incoming_date is distinct from new.incoming_date
                        or
                        units.last_operation_outgoing_date is distinct from new.outgoing_date
                    );
            end if;
        end if;

        if deleted_units_ids is not null then
            update units set
                (
                    __last_operation_id,
                    last_operation_incoming_date,
                    last_operation_outgoing_date
                ) = (
                    select
                            last_operation.id as __last_operation_id,
                            last_operation.incoming_date as last_operation_incoming_date,
                            last_operation.outgoing_date as last_operation_outgoing_date
                    from operations as last_operation
                    where
                        last_operation.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        last_operation.id desc nulls first
                    limit 1
                )
            where
                units.id = any( deleted_units_ids )
                and
                units.__last_operation_id = new.id;
        end if;

        if inserted_units_ids is not null then
            update units set
                __last_operation_id = new.id,
                last_operation_incoming_date = new.incoming_date,
                last_operation_outgoing_date = new.outgoing_date
            where
                units.id = any( inserted_units_ids )
                and
                (
                    units.__last_operation_id is null
                    or
                    units.__last_operation_id < new.id
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.units_ids is not null then
            update units set
                __last_operation_id = new.id,
                last_operation_incoming_date = new.incoming_date,
                last_operation_outgoing_date = new.outgoing_date
            where
                units.id = any( new.units_ids );

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_operation_for_units_on_operations
after insert or update of incoming_date, outgoing_date, units_ids or delete
on public.operations
for each row
execute procedure cache_last_operation_for_units_on_operations();