cache renomination for invoice (
    select
        sum( renomination_invoice.sum ) as renomination_sum,

        string_agg(
            renomination_invoice.account_no_doc_number, ', '
        ) as renomination_link,

        string_agg(
            distinct list_currency.charcode, ', '
        ) as renomination_currencies

    FROM invoice AS renomination_invoice

    left join list_currency on
        list_currency.id = renomination_invoice.id_list_currency

    WHERE
        renomination_invoice.id = ANY( invoice.renomination_invoices )
)
without triggers on list_currency