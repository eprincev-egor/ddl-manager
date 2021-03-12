create or replace function cache_first_auto_for_order_on_operations()
returns trigger as $body$
declare prev_row record;
declare prev_id bigint;
begin

    if TG_OP = 'DELETE' then
        if
            old.id_order is not null
            and
            old.type = 'auto'
            and
            old.deleted = 0
        then
            if not old.__first_auto_for_order then
                return old;
            end if;

            select
                deleted,
                doc_number,
                id,
                id_order,
                incoming_date,
                type
            from operations
            where
                operations.id_order = old.id_order
                and
                operations.type = 'auto'
                and
                operations.deleted = 0
            order by
                operations.id asc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update operations set
                    __first_auto_for_order = true
                where
                    operations.id = prev_row.id;
            end if;

            update public.order set
                first_auto_number = prev_row.doc_number,
                first_incoming_date = prev_row.incoming_date
            where
                old.id_order = public.order.id
                and
                (
                    public.order.first_auto_number is distinct from prev_row.doc_number
                    or
                    public.order.first_incoming_date is distinct from prev_row.incoming_date
                );

        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.doc_number is not distinct from old.doc_number
            and
            new.id_order is not distinct from old.id_order
            and
            new.incoming_date is not distinct from old.incoming_date
            and
            new.type is not distinct from old.type
        then
            return new;
        end if;

        if
            new.id_order is not distinct from old.id_order
            and
            new.type is not distinct from old.type
            and
            new.deleted is not distinct from old.deleted
        then
            if not new.__first_auto_for_order then
                return new;
            end if;

            update public.order set
                first_auto_number = new.doc_number,
                first_incoming_date = new.incoming_date
            where
                new.id_order = public.order.id
                and
                (
                    public.order.first_auto_number is distinct from new.doc_number
                    or
                    public.order.first_incoming_date is distinct from new.incoming_date
                );

            return new;
        end if;

        if
            old.id_order is not null
            and
            old.type = 'auto'
            and
            old.deleted = 0
            and
            old.__first_auto_for_order
        then
            select
                deleted,
                doc_number,
                id,
                id_order,
                incoming_date,
                type
            from operations
            where
                operations.id_order = old.id_order
                and
                operations.type = 'auto'
                and
                operations.deleted = 0
            order by
                operations.id asc nulls first
            limit 1
            into prev_row;

            if prev_row.id is not null then
                update operations set
                    __first_auto_for_order = true
                where
                    operations.id = prev_row.id;
            end if;

            update public.order set
                first_auto_number = prev_row.doc_number,
                first_incoming_date = prev_row.incoming_date
            where
                old.id_order = public.order.id
                and
                (
                    public.order.first_auto_number is distinct from prev_row.doc_number
                    or
                    public.order.first_incoming_date is distinct from prev_row.incoming_date
                );
        end if;

        if
            new.id_order is not null
            and
            new.type = 'auto'
            and
            new.deleted = 0
        then
            prev_id = (
                select
                    min( operations.id )
                from operations
                where
                    operations.id_order = new.id_order
                    and
                    operations.type = 'auto'
                    and
                    operations.deleted = 0
                    and
                    operations.id <> new.id
            );

            if
                prev_id > new.id
                or
                prev_id is null
            then
                if prev_id is not null then
                    update operations set
                        __first_auto_for_order = false
                    where
                        operations.id = prev_id
                        and
                        __first_auto_for_order = true;
                end if;

                if not new.__first_auto_for_order then
                    update operations set
                        __first_auto_for_order = true
                    where
                        operations.id = new.id;
                end if;

                update public.order set
                    first_auto_number = new.doc_number,
                    first_incoming_date = new.incoming_date
                where
                    new.id_order = public.order.id
                    and
                    (
                        public.order.first_auto_number is distinct from new.doc_number
                        or
                        public.order.first_incoming_date is distinct from new.incoming_date
                    );
            end if;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if
            new.id_order is not null
            and
            new.type = 'auto'
            and
            new.deleted = 0
        then
            update operations set
                __first_auto_for_order = false
            where
                operations.id_order = new.id_order
                and
                operations.id < new.id
                and
                __first_auto_for_order = true;

            update public.order set
                first_auto_number = new.doc_number,
                first_incoming_date = new.incoming_date
            where
                new.id_order = public.order.id
                and
                (
                    public.order.first_auto_number is distinct from new.doc_number
                    or
                    public.order.first_incoming_date is distinct from new.incoming_date
                );

        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_first_auto_for_order_on_operations
after insert or update of deleted, doc_number, id_order, incoming_date, type or delete
on public.operations
for each row
execute procedure cache_first_auto_for_order_on_operations();