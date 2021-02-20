create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_order_type_name text;
declare new_order_type_name text;
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.deleted = 0
        then
            if old.id_order_type is not null then
                old_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = old.id_order_type
                );
            end if;

            if old_order_type_name in ('LCL', 'LTL') then
                update companies set
                    orders_total = orders_total - coalesce(old.profit, 0)
                where
                    old.id_client = companies.id;
            end if;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_client is not distinct from old.id_client
            and
            new.id_order_type is not distinct from old.id_order_type
            and
            new.profit is not distinct from old.profit
        then
            return new;
        end if;

        if old.id_order_type is not null then
            old_order_type_name = (
                select
                    order_type.name
                from order_type
                where
                    order_type.id = old.id_order_type
            );
        end if;

        if new.id_order_type is not distinct from old.id_order_type then
            new_order_type_name = old_order_type_name;
        else
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;
        end if;

        if
            new.id_client is not distinct from old.id_client
            and
            new.deleted is not distinct from old.deleted
            and
            new_order_type_name is not distinct from old_order_type_name
        then
            if
                not coalesce(new.deleted = 0, false)
                or
                not coalesce(new_order_type_name in ('LCL', 'LTL'), false)
            then
                return new;
            end if;

            update companies set
                orders_total = orders_total - coalesce(old.profit, 0) + coalesce(new.profit, 0)
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.deleted = 0
            and
            old_order_type_name in ('LCL', 'LTL')
        then
            update companies set
                orders_total = orders_total - coalesce(old.profit, 0)
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            new.deleted = 0
            and
            new_order_type_name in ('LCL', 'LTL')
        then
            update companies set
                orders_total = orders_total + coalesce(new.profit, 0)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            new.deleted = 0
        then
            if new.id_order_type is not null then
                new_order_type_name = (
                    select
                        order_type.name
                    from order_type
                    where
                        order_type.id = new.id_order_type
                );
            end if;

            if new_order_type_name in ('LCL', 'LTL') then
                update companies set
                    orders_total = orders_total + coalesce(new.profit, 0)
                where
                    new.id_client = companies.id;
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of deleted, id_client, id_order_type, profit or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();