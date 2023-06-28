create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare new_row record;
declare old_row record;
declare return_row record;
begin
    if TG_OP = 'DELETE' then
        return_row = old;
    else
        return_row = new;
    end if;

    new_row = return_row;
    old_row = return_row;

    if TG_OP in ('INSERT', 'UPDATE') then
        new_row = new;
    end if;
    if TG_OP in ('UPDATE', 'DELETE') then
        old_row = old;
    end if;

    with
        changed_rows as (
            select old_row.id, old_row.order_date, old_row.order_number
            union
            select new_row.id, new_row.order_date, new_row.order_number
        )
    update companies set
        (
            max_order_date,
            orders_numbers,
            __totals_json__
        ) = (
            select
                    max(orders.order_date) as max_order_date,
                    string_agg(distinct 
                        orders.order_number,
                        ', '
                                        ) as orders_numbers,
                    ('{' || string_agg(
                                                    '"' || link.id::text || '":' || jsonb_build_object(
                                'id', link.id,'id_company', link.id_company,'id_order', link.id_order
                            )::text,
                                                    ','
                                                ) || '}')
                    ::
                    jsonb as __totals_json__
            from order_company_link as link

            left join orders on
                orders.id = link.id_order
            where
                link.id_company = companies.id
        )
    from changed_rows, order_company_link as link
    where
        link.id_company = companies.id
        and
        changed_rows.id = link.id_order;

    return return_row;
end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of order_date, order_number or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();