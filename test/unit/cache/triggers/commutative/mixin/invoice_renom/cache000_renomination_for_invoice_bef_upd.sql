create or replace function cache000_renomination_for_invoice_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if cm_equal_arrays(new.renomination_invoices, old.renomination_invoices) then
        return new;
    end if;


    select
        sum(renomination_invoice.sum) as renomination_sum,
        array_agg(
            renomination_invoice.account_no_doc_number
        ) as renomination_link_account_no_doc_number,
        string_agg(
            renomination_invoice.account_no_doc_number,
            ', '
        ) as renomination_link,
        array_agg(list_currency.charcode) as renomination_currencies_charcode,
        string_agg(distinct 
            list_currency.charcode,
            ', '
        ) as renomination_currencies
    from invoice as renomination_invoice left join list_currency on
list_currency.id = renomination_invoice.id_list_currency
    where
        renomination_invoice.id = any (new.renomination_invoices)
    into new_totals;


    new.renomination_sum = new_totals.renomination_sum;
    new.renomination_link_account_no_doc_number = new_totals.renomination_link_account_no_doc_number;
    new.renomination_link = new_totals.renomination_link;
    new.renomination_currencies_charcode = new_totals.renomination_currencies_charcode;
    new.renomination_currencies = new_totals.renomination_currencies;


    return new;
end
$body$
language plpgsql;

create trigger cache000_renomination_for_invoice_bef_upd
before update of renomination_invoices
on public.invoice
for each row
execute procedure cache000_renomination_for_invoice_bef_upd();