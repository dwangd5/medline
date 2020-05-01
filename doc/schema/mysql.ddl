create table articles (
pmid varchar(255) unique not null,
pmc varchar(255),
doi varchar(255),
other_id varchar(255),
title varchar(1000),
abstract text,
authors varchar(1000),
affiliations varchar(1000),
mesh_terms varchar(1000),
publication_types varchar(1000),
keywords varchar(1000),
chemical_list varchar(1000),
pubdate varchar(10),
pubyear int,
journal varchar(1000),
medline_ta varchar(1000),
nlm_unique_id varchar(255),
issn_linking varchar(255),
country varchar(255),
references varchar(1000),
deleteflag boolean,
primary key(pmid)
);


create table update_status (
file_name varchar(255),
update_date varchar(50),
status bool,
primary key(file_name)
)
