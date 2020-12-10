create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies set
                orders_dates_array_agg = cm_array_remove_one_element(
                    orders_dates_array_agg,
                    old.order_date
                ),
                orders_dates = cm_distinct_array(
                    cm_array_remove_one_element(
                        orders_dates_array_agg,
                        old.order_date
                    )
                )
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
                orders_dates_array_agg = cm_array_append_order_by_desc_nulls_first(
                    cm_array_remove_one_element(
                        orders_dates_array_agg,
                        old.order_date
                    ),
                    new.order_date
                ),
                orders_dates = cm_distinct_array(
                    cm_array_append_order_by_desc_nulls_first(
                        cm_array_remove_one_element(
                            orders_dates_array_agg,
                            old.order_date
                        ),
                        new.order_date
                    )
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                orders_dates_array_agg = cm_array_remove_one_element(
                    orders_dates_array_agg,
                    old.order_date
                ),
                orders_dates = cm_distinct_array(
                    cm_array_remove_one_element(
                        orders_dates_array_agg,
                        old.order_date
                    )
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                orders_dates_array_agg = cm_array_append_order_by_desc_nulls_first(
                    orders_dates_array_agg,
                    new.order_date
                ),
                orders_dates = cm_distinct_array(
                    cm_array_append_order_by_desc_nulls_first(
                        orders_dates_array_agg,
                        new.order_date
                    )
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies set
                orders_dates_array_agg = cm_array_append_order_by_desc_nulls_first(
                    orders_dates_array_agg,
                    new.order_date
                ),
                orders_dates = cm_distinct_array(
                    cm_array_append_order_by_desc_nulls_first(
                        orders_dates_array_agg,
                        new.order_date
                    )
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