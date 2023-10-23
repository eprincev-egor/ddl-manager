create or replace function cache000_renomination_for_invoice_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
            ('{' || string_agg(
                                        '"' || renomination_invoice.id::text || '":' || jsonb_build_object(
                            'account_no_doc_number', renomination_invoice.account_no_doc_number,'id', renomination_invoice.id,'id_list_currency', renomination_invoice.id_list_currency,'sum', renomination_invoice.sum
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __renomination_json__,
            string_agg(distinct 
                list_currency.charcode,
                ', '
                        ) as renomination_currencies,
            string_agg(
                renomination_invoice.account_no_doc_number,
                ', '
                        ) as renomination_link,
            sum(renomination_invoice.sum) as renomination_sum
    from invoice as renomination_invoice

    left join list_currency on
        list_currency.id = renomination_invoice.id_list_currency
    where
        renomination_invoice.id = any(new.renomination_invoices)
    into new_totals;


    new.__renomination_json__ = new_totals.__renomination_json__;
    new.renomination_currencies = new_totals.renomination_currencies;
    new.renomination_link = new_totals.renomination_link;
    new.renomination_sum = new_totals.renomination_sum;


    return new;
end
$body$
language plpgsql;

create trigger cache000_renomination_for_invoice_bef_ins
before insert
on public.invoice
for each row
execute procedure cache000_renomination_for_invoice_bef_ins();