create or replace function cache_totals_for_some_report_row_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update some_report_row set
            orders_count = orders_count - 1;

        return old;
    end if;


    if TG_OP = 'INSERT' then

        update some_report_row set
            orders_count = orders_count + 1;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_some_report_row_on_orders
after insert or delete
on public.orders
for each row
execute procedure cache_totals_for_some_report_row_on_orders();