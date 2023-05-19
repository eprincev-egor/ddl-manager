create or replace function cache000_rates_total_bef_ins()
returns trigger as $body$
begin
    new.total = new.quantity * new.price * calc_vat(
        new.vat_type,
        new.vat_value
    );

    return new;
end
$body$
language plpgsql;

create trigger cache000_rates_total_bef_ins
before insert
on public.rates
for each row
execute procedure cache000_rates_total_bef_ins();