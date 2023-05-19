create or replace function cache_totals_for_self_on_companies()
returns trigger as $body$
begin

    if new.id_country is not distinct from old.id_country then
        return new;
    end if;


    update companies set
        (
            orders_numbers_doc_number,
            orders_numbers
        ) = (
            select
                array_agg(orders.doc_number) as orders_numbers_doc_number,
                string_agg(distinct orders.doc_number, ', ') as orders_numbers

            from orders

            where
                orders.id_country = companies.id_country
        )
    where
        public.companies.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_companies
after update of id_country
on public.companies
for each row
execute procedure cache_totals_for_self_on_companies();