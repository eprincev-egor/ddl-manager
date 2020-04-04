create table country (
    id integer primary key,
    code text not null
);

create table company (
    id integer primary key,
    id_country integer not null,
    constraint company_country_fk 
        foreign key (id_country) 
        references country
);