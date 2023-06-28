create or replace function cache000_renomination_for_invoice_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if cm_equal_arrays(new.renomination_invoices, old.renomination_invoices) then
        return new;
    end if;


    select
            sum(renomination_invoice.sum) as renomination_sum,
            string_agg(
                renomination_invoice.account_no_doc_number,
                ', '
                        ) as renomination_link,
            string_agg(distinct 
                list_currency.charcode,
                ', '
                        ) as renomination_currencies,
            ('{' || string_agg(
                                            '"' || renomination_invoice.id::text || '":' || jsonb_build_object(
                        'account_no_doc_number', renomination_invoice.account_no_doc_number,'id', renomination_invoice.id,'id_list_currency', renomination_invoice.id_list_currency,'sum', renomination_invoice.sum
                    )::text,
                                            ','
                                        ) || '}')
            ::
            jsonb as __renomination_json__
    from invoice as renomination_invoice

    left join list_currency on
        list_currency.id = renomination_invoice.id_list_currency
    where
        renomination_invoice.id = any (new.renomination_invoices)
    into new_totals;


    new.renomination_sum = new_totals.renomination_sum;
    new.renomination_link = new_totals.renomination_link;
    new.renomination_currencies = new_totals.renomination_currencies;
    new.__renomination_json__ = new_totals.__renomination_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_renomination_for_invoice_bef_upd
before update of renomination_invoices
on public.invoice
for each row
execute procedure cache000_renomination_for_invoice_bef_upd();