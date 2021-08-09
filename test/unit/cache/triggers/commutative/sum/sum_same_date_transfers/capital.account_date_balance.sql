create or replace function cache_totals_for_self_on_account_date_balance()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if new.balance_date is null then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if new.balance_date is not distinct from old.balance_date then
            return new;
        end if;
    end if;


    update capital.account_date_balance as date_balance set
        (
            transfers_sum
        ) = (
            select
                sum(capital.transfer.delta) as transfers_sum

            from capital.transfer

            where
                capital.transfer.transfer_date = date_balance.balance_date
        )
    where
        date_balance.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_account_date_balance
after insert or update of balance_date
on capital.account_date_balance
for each row
execute procedure cache_totals_for_self_on_account_date_balance();