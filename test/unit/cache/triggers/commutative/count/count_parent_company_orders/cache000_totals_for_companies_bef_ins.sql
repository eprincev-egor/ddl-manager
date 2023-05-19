create or replace function cache000_totals_for_companies_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
        count(*) as parent_company_orders_count
    from orders
    where
        orders.id_client = new.id_parent_company
    into new_totals;


    new.parent_company_orders_count = new_totals.parent_company_orders_count;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_companies_bef_ins
before insert
on public.companies
for each row
execute procedure cache000_totals_for_companies_bef_ins();