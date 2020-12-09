create or replace function cache_operation_units_for_unit_on_unit()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_owner_unit is not null
            and
            old.deleted = 0
        then
            update owner.unit set
                operation_units_ids = cm_array_remove_one_element(
                    operation_units_ids,
                    old.id
                )
            where
                old.id_owner_unit = owner.unit.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_owner_unit is not distinct from old.id_owner_unit
        then
            return new;
        end if;



        if
            old.id_owner_unit is not null
            and
            old.deleted = 0
        then
            update owner.unit set
                operation_units_ids = cm_array_remove_one_element(
                    operation_units_ids,
                    old.id
                )
            where
                old.id_owner_unit = owner.unit.id;
        end if;

        if
            new.id_owner_unit is not null
            and
            new.deleted = 0
        then
            update owner.unit set
                operation_units_ids = array_append(
                    operation_units_ids,
                    new.id
                )
            where
                new.id_owner_unit = owner.unit.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_owner_unit is not null
            and
            new.deleted = 0
        then
            update owner.unit set
                operation_units_ids = array_append(
                    operation_units_ids,
                    new.id
                )
            where
                new.id_owner_unit = owner.unit.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_operation_units_for_unit_on_unit
after insert or update of deleted, id_owner_unit or delete
on operation.unit
for each row
execute procedure cache_operation_units_for_unit_on_unit();