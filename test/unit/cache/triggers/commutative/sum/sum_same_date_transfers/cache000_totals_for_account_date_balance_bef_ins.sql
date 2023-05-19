create or replace function cache000_totals_for_account_date_balance_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



    select
        sum(capital.transfer.delta) as transfers_sum
    from capital.transfer
    where
        capital.transfer.transfer_date = new.balance_date
    into new_totals;


    new.transfers_sum = new_totals.transfers_sum;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_account_date_balance_bef_ins
before insert
on capital.account_date_balance
for each row
execute procedure cache000_totals_for_account_date_balance_bef_ins();