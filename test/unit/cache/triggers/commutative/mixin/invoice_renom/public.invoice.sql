create or replace function cache_renomination_for_invoice_on_invoice()
returns trigger as $body$
declare old_list_currency_charcode text;
declare new_list_currency_charcode text;
begin

    if TG_OP = 'DELETE' then

        if old.id_list_currency is not null then
            old_list_currency_charcode = (
                select
                    list_currency.charcode
                from list_currency
                where
                    list_currency.id = old.id_list_currency
            );
        end if;

        update invoice set
            renomination_sum = renomination_sum - coalesce(old.sum, 0),
            renomination_link_account_no_doc_number = cm_array_remove_one_element(
                renomination_link_account_no_doc_number,
                old.account_no_doc_number
            ),
            renomination_link = (
                select
                    string_agg(
                        item.account_no_doc_number,
                        ', '
                    )

                from unnest(
                    cm_array_remove_one_element(
                        renomination_link_account_no_doc_number,
                        old.account_no_doc_number
                    )
                ) as item(account_no_doc_number)
            ),
            renomination_currencies_charcode = cm_array_remove_one_element(
                renomination_currencies_charcode,
                old_list_currency_charcode
            ),
            renomination_currencies = (
                select
                    string_agg(distinct item.charcode, ', ')

                from unnest(
                    cm_array_remove_one_element(
                        renomination_currencies_charcode,
                        old_list_currency_charcode
                    )
                ) as item(charcode)
            )
        where
            invoice.renomination_invoices && ARRAY[ old.id ]::int8[];

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.account_no_doc_number is not distinct from old.account_no_doc_number
            and
            new.id_list_currency is not distinct from old.id_list_currency
            and
            new.sum is not distinct from old.sum
        then
            return new;
        end if;

        if old.id_list_currency is not null then
            old_list_currency_charcode = (
                select
                    list_currency.charcode
                from list_currency
                where
                    list_currency.id = old.id_list_currency
            );
        end if;

        if new.id_list_currency is not distinct from old.id_list_currency then
            new_list_currency_charcode = old_list_currency_charcode;
        else
            if new.id_list_currency is not null then
                new_list_currency_charcode = (
                    select
                        list_currency.charcode
                    from list_currency
                    where
                        list_currency.id = new.id_list_currency
                );
            end if;
        end if;

        update invoice set
            renomination_sum = renomination_sum - coalesce(old.sum, 0) + coalesce(new.sum, 0),
            renomination_link_account_no_doc_number = array_append(
                cm_array_remove_one_element(
                    renomination_link_account_no_doc_number,
                    old.account_no_doc_number
                ),
                new.account_no_doc_number
            ),
            renomination_link = (
                select
                    string_agg(
                        item.account_no_doc_number,
                        ', '
                    )

                from unnest(
                    array_append(
                        cm_array_remove_one_element(
                            renomination_link_account_no_doc_number,
                            old.account_no_doc_number
                        ),
                        new.account_no_doc_number
                    )
                ) as item(account_no_doc_number)
            ),
            renomination_currencies_charcode = array_append(
                cm_array_remove_one_element(
                    renomination_currencies_charcode,
                    old_list_currency_charcode
                ),
                new_list_currency_charcode
            ),
            renomination_currencies = (
                select
                    string_agg(distinct item.charcode, ', ')

                from unnest(
                    array_append(
                        cm_array_remove_one_element(
                            renomination_currencies_charcode,
                            old_list_currency_charcode
                        ),
                        new_list_currency_charcode
                    )
                ) as item(charcode)
            )
        where
            invoice.renomination_invoices && ARRAY[ new.id ]::int8[];



        return new;
    end if;
end
$body$
language plpgsql;

create trigger cache_renomination_for_invoice_on_invoice
after update of account_no_doc_number, id_list_currency, sum or delete
on public.invoice
for each row
execute procedure cache_renomination_for_invoice_on_invoice();