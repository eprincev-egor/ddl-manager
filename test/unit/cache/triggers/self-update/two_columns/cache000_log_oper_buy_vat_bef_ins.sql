create or replace function cache000_log_oper_buy_vat_bef_ins()
returns trigger as $body$
begin
    new.buy_vat = calc_vat(
        new.buy_vat_type,
        new.buy_vat_value
    );

    return new;
end
$body$
language plpgsql;

create trigger cache000_log_oper_buy_vat_bef_ins
before insert
on public.log_oper
for each row
execute procedure cache000_log_oper_buy_vat_bef_ins();