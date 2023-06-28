create or replace function cache_last_auto_doc_for_units_on_operations()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then
        if
            old.id_doc_parent_operation is not null
            and
            old.units_ids is not null
            and
            old.id_operation_type = 1
            and
            old.deleted = 0
        then
            update units set
                (
                    __last_auto_doc_id,
                    id_last_auto_doc
                ) = (
                    select
                            last_auto_doc.id as __last_auto_doc_id,
                            last_auto_doc.id as id_last_auto_doc
                    from operations as last_auto_doc
                    where
                        last_auto_doc.id_doc_parent_operation = units.id_last_auto
                        and
                        last_auto_doc.id_operation_type = 1
                        and
                        last_auto_doc.deleted = 0
                        and
                        last_auto_doc.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        last_auto_doc.id desc nulls first
                    limit 1
                )
            where
                units.id = any( old.units_ids )
                and
                units.__last_auto_doc_id = old.id;

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_doc_parent_operation is not distinct from old.id_doc_parent_operation
            and
            new.id_operation_type is not distinct from old.id_operation_type
            and
            cm_equal_arrays(new.units_ids, old.units_ids)
        then
            return new;
        end if;

        matched_old = coalesce(old.id_operation_type = 1
            and
            old.deleted = 0, false);
        matched_new = coalesce(new.id_operation_type = 1
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
            if new.id is distinct from old.id then
                update units set
                    __last_auto_doc_id = new.id,
                    id_last_auto_doc = new.id
                where
                    units.id = any( not_changed_units_ids )
                    and
                    units.__last_auto_doc_id = new.id
                    and
                    units.id_last_auto_doc is distinct from new.id;
            end if;
        end if;

        if deleted_units_ids is not null then
            update units set
                (
                    __last_auto_doc_id,
                    id_last_auto_doc
                ) = (
                    select
                            last_auto_doc.id as __last_auto_doc_id,
                            last_auto_doc.id as id_last_auto_doc
                    from operations as last_auto_doc
                    where
                        last_auto_doc.id_doc_parent_operation = units.id_last_auto
                        and
                        last_auto_doc.id_operation_type = 1
                        and
                        last_auto_doc.deleted = 0
                        and
                        last_auto_doc.units_ids && ARRAY[units.id] :: bigint[]
                    order by
                        last_auto_doc.id desc nulls first
                    limit 1
                )
            where
                units.id = any( deleted_units_ids )
                and
                units.__last_auto_doc_id = new.id;
        end if;

        if inserted_units_ids is not null then
            update units set
                __last_auto_doc_id = new.id,
                id_last_auto_doc = new.id
            where
                new.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( inserted_units_ids )
                and
                (
                    units.__last_auto_doc_id is null
                    or
                    units.__last_auto_doc_id < new.id
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.id_doc_parent_operation is not null
            and
            new.units_ids is not null
            and
            new.id_operation_type = 1
            and
            new.deleted = 0
        then
            update units set
                __last_auto_doc_id = new.id,
                id_last_auto_doc = new.id
            where
                new.id_doc_parent_operation = units.id_last_auto
                and
                units.id = any( new.units_ids );

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_last_auto_doc_for_units_on_operations
after insert or update of deleted, id_doc_parent_operation, id_operation_type, units_ids or delete
on public.operations
for each row
execute procedure cache_last_auto_doc_for_units_on_operations();