create or replace function cache000_totals_for_companies_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            ('{' || string_agg(
                                        '"' || public.orders.id::text || '":' || jsonb_build_object(
                            'doc_number', public.orders.doc_number,'id', public.orders.id,'id_country', public.orders.id_country
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __totals_json__,
            string_agg(distinct orders.doc_number, ', ') as orders_numbers
    from orders
    where
        orders.id_country = new.id_country
    into new_totals;


    new.__totals_json__ = new_totals.__totals_json__;
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