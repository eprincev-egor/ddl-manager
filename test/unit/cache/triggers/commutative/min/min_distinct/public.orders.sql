create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            old.order_date is not null
        then
            update companies set
                min_order_date = case
                    when
                        min_order_date < old.order_date
                    then
                        min_order_date
                    else
                        (
                            select
                                min(orders.order_date) as min_order_date
                            from orders
                            where
                                orders.id_client = companies.id
                        )
                end
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.order_date is not distinct from old.order_date
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                min_order_date = case
                    when
                        new.order_date < min_order_date
                    then
                        new.order_date
                    when
                        old.order_date > min_order_date
                    then
                        min_order_date
                    else
                        (
                            select
                                min(orders.order_date) as min_order_date
                            from orders
                            where
                                orders.id_client = companies.id
                        )
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            old.order_date is not null
        then
            update companies set
                min_order_date = case
                    when
                        min_order_date < old.order_date
                    then
                        min_order_date
                    else
                        (
                            select
                                min(orders.order_date) as min_order_date
                            from orders
                            where
                                orders.id_client = companies.id
                        )
                end
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            new.order_date is not null
        then
            update companies set
                min_order_date = least(
                    min_order_date,
                    new.order_date
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            new.order_date is not null
        then
            update companies set
                min_order_date = least(
                    min_order_date,
                    new.order_date
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, order_date or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();