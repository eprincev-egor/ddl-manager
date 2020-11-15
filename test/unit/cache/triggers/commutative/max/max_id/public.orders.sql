create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                max_order_id = case
                    when
                        max_order_id > old.id
                    then
                        max_order_id
                    else
                        (
                            select
                                max(orders.id) as max_order_id
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
        if new.id_client is not distinct from old.id_client then
            return new;
        end if;



        if old.id_client is not null then
            update companies set
                max_order_id = case
                    when
                        max_order_id > old.id
                    then
                        max_order_id
                    else
                        (
                            select
                                max(orders.id) as max_order_id
                            from orders
                            where
                                orders.id_client = companies.id
                        )
                end
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                max_order_id = greatest(max_order_id, new.id)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                max_order_id = greatest(max_order_id, new.id)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();