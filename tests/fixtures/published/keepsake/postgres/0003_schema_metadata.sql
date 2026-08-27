create table keepsake_schema_metadata (
  key text primary key,
  value text not null
);

insert into keepsake_schema_metadata (key, value)
values ('backend', 'postgres');
