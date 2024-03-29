create database pubmed;

create table base (
pmid string,
pmc string,
doi string,
other_id string,
title string,
abstract string,
authors string,
affiliations string,
mesh_terms string,
publication_types string,
keywords string,
chemical_list string,
pubdate string,
pubyear int,
journal string,
medline_ta string,
nlm_unique_id string,
issn_linking string,
country string,
`references` string,
deleteflag boolean
);

create table update2024 (
pmid string,
pmc string,
doi string,
other_id string,
title string,
abstract string,
authors string,
affiliations string,
mesh_terms string,
publication_types string,
keywords string,
chemical_list string,
pubdate string,
pubyear int,
submitted_date string,
journal string,
medline_ta string,
nlm_unique_id string,
issn_linking string,
country string,
`references` string,
deleteflag boolean,
file_name string
);


