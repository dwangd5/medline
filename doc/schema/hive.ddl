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

create table update2020 (
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

# create consolidated table based on base and update2020:
# memory out. need to fine tune memory
create table medline as
select * from
(select * from base union select * from update2020) u
where u.pmid not in
(select pmid from update2020 where deleteflag is true);