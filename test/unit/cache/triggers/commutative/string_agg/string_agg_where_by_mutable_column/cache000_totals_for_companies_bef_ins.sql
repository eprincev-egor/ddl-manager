create or replace function cache000_totals_for_companies_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
        array_agg(orders.doc_number) as orders_numbers_doc_number,
        string_agg(distinct orders.doc_number, ', ') as orders_numbers
    from orders
    where
        orders.id_country = new.id_country
    into new_totals;


    new.orders_numbers_doc_number = new_totals.orders_numbers_doc_number;
    new.orders_numbers = new_totals.orders_numbers;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_companies_bef_ins
before insert
on public.companies
for each row
execute procedure cache000_totals_for_companies_bef_ins();