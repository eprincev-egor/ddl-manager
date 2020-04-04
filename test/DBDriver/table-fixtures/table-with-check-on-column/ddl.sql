create table orders (
    id integer,
    profit integer check( profit > 100 )
)