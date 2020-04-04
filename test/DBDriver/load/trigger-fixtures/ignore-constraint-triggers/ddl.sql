create table test (
    id serial primary key
);

create table test_units (
    id serial primary key,
-- foreign key creating system constraint trigger
-- don't load it!
    id_parent integer not null
        references test
            on update cascade
            on delete cascade
);