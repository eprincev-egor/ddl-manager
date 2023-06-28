create or replace function cache_operation_for_comments_on_operation()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if
            (
                coalesce(
                    old.doc_parent_id_order,
                    old.id_order
                ) is not null
                or
                old.id_operation_type is not null
            )
            and
            old.deleted = 0
        then
            update comments set
                operation_id_order = coalesce(
                    (null::bigint),
                    (null::bigint)
                                ),
                operation_type_id = null
            where
                old.id = comments.row_id
                and
                comments.query_name = 'OPERATION'
                and
                (
                    comments.operation_id_order is distinct from (coalesce(
                        (null::bigint),
                        (null::bigint)
                    ))
                    or
                    comments.operation_type_id is distinct from (null)
                );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.doc_parent_id_order is not distinct from old.doc_parent_id_order
            and
            new.id_operation_type is not distinct from old.id_operation_type
            and
            new.id_order is not distinct from old.id_order
        then
            return new;
        end if;

        if
            coalesce(old.deleted = 0, false)
            or
            coalesce(new.deleted = 0, false)
        then
            update comments set
                operation_id_order = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        coalesce(
                            new.doc_parent_id_order,
                            new.id_order
                                                )
                    else
                        null
                end,
                operation_type_id = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        new.id_operation_type
                    else
                        null
                end
            where
                new.id = comments.row_id
                and
                comments.query_name = 'OPERATION'
                and
                (
                    comments.operation_id_order is distinct from (case
                        when
                            coalesce(new.deleted = 0, false)
                        then
                            coalesce(
                                new.doc_parent_id_order,
                                new.id_order
                                    )
                        else
                            null
                    end)
                    or
                    comments.operation_type_id is distinct from (case
                        when
                            coalesce(new.deleted = 0, false)
                        then
                            new.id_operation_type
                        else
                            null
                    end)
                );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            (
                coalesce(
                    new.doc_parent_id_order,
                    new.id_order
                ) is not null
                or
                new.id_operation_type is not null
            )
            and
            new.deleted = 0
        then
            update comments set
                operation_id_order = coalesce(
                    new.doc_parent_id_order,
                    new.id_order
                                ),
                operation_type_id = new.id_operation_type
            where
                new.id = comments.row_id
                and
                comments.query_name = 'OPERATION'
                and
                (
                    comments.operation_id_order is distinct from (coalesce(
                        new.doc_parent_id_order,
                        new.id_order
                    ))
                    or
                    comments.operation_type_id is distinct from (new.id_operation_type)
                );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_operation_for_comments_on_operation
after insert or update of deleted, doc_parent_id_order, id_operation_type, id_order or delete
on operation.operation
for each row
execute procedure cache_operation_for_comments_on_operation();