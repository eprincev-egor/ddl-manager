create table company (
    id integer,
    name text,
    constraint company_unique_name UNIQUE (name)
)