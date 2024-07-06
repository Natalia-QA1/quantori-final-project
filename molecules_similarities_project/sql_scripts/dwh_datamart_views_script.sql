-- 1
create or replace view nananeva.avg_similarity_score_per_source_molecule as
select source_molecule_reference ,
	   avg(tanimoto_similarity_score) as avg_similarity_score
from nananeva.dm_top10_fct_molecules_similarities
group by source_molecule_reference ;


-- 2
create or replace view nananeva.avg_alogp_deviation as
with source_alogp as (
    select
        f.source_molecule_reference as source_mol,
        d.alogp as source_alogp
    from nananeva.dm_top10_fct_molecules_similarities f
    join nananeva.dm_top10_dim_molecules d
    on f.source_molecule_reference = d.chembl_id
    group by
        f.source_molecule_reference,
        d.alogp
),
target_alogp as (
    select
        f.target_molecule_reference as target_mol,
        d.alogp as target_alogp
    from nananeva.dm_top10_fct_molecules_similarities f
    join nananeva.dm_top10_dim_molecules d
    on f.target_molecule_reference = d.chembl_id
    group by
        f.target_molecule_reference,
        d.alogp
)
select
    fct.source_molecule_reference,
    avg(abs(tgt.target_alogp - src.source_alogp)) as avg_alogp_deviation
from nananeva.dm_top10_fct_molecules_similarities fct
join target_alogp tgt
on  fct.target_molecule_reference = tgt.target_mol
join source_alogp src
on  fct.source_molecule_reference = src.source_mol
group by  fct.source_molecule_reference;


-- 3
create or replace view nananeva.pivot_10_random_source_molecules as
 select
    target_molecule_reference,
    max(case when source_molecule_reference = 'CHEMBL140380' then tanimoto_similarity_score end) as "CHEMBL140380",
    max(case when source_molecule_reference = 'CHEMBL269729' then tanimoto_similarity_score end) as "CHEMBL269729",
    max(case when source_molecule_reference = 'CHEMBL269758' then tanimoto_similarity_score end) as "CHEMBL269758",
    max(case when source_molecule_reference = 'CHEMBL27121' then tanimoto_similarity_score end) as "CHEMBL27121",
    max(case when source_molecule_reference = 'CHEMBL414181' then tanimoto_similarity_score end) as "CHEMBL414181",
    max(case when source_molecule_reference = 'CHEMBL429017' then tanimoto_similarity_score end) as "CHEMBL429017",
    max(case when source_molecule_reference = 'CHEMBL586106' then tanimoto_similarity_score end) as "CHEMBL586106",
    max(case when source_molecule_reference = 'CHEMBL6222' then tanimoto_similarity_score end) as "CHEMBL6222",
    max(case when source_molecule_reference = 'CHEMBL6240' then tanimoto_similarity_score end) as "CHEMBL6240",
    max(case when source_molecule_reference = 'CHEMBL6334' then tanimoto_similarity_score end) as "CHEMBL6334"
from
    nananeva.dm_top10_fct_molecules_similarities
where  source_molecule_reference in
    ('CHEMBL140380',
    'CHEMBL269729',
    'CHEMBL269758',
    'CHEMBL27121',
    'CHEMBL414181',
    'CHEMBL429017',
    'CHEMBL586106',
    'CHEMBL6222',
    'CHEMBL6240',
    'CHEMBL6334')
group by  target_molecule_reference
order by  target_molecule_reference;

/*
create extension if not exists tablefunc;

create or replace view nananeva.pivot_10_random_source_molecules as
select *
from crosstab(
    $$
    select target_molecule_reference, source_molecule_reference, tanimoto_similarity_score
    from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024
    where source_molecule_reference in
    ('CHEMBL1521628',
    'CHEMBL1521632',
    'CHEMBL1521690',
    'CHEMBL1521773',
    'CHEMBL1521808',
    'CHEMBL1521873',
    'CHEMBL4462956',
    'CHEMBL4463317',
    'CHEMBL4463346',
    'CHEMBL4472235')
    order by 1, 2
    $$
) as ct (
    target_molecule_reference text,
    ('CHEMBL1521628', float
    'CHEMBL1521632', float
    'CHEMBL1521690', float
    'CHEMBL1521773', float
    'CHEMBL1521808', float
    'CHEMBL1521873', float
    'CHEMBL4462956', float
    'CHEMBL4463317', float
    'CHEMBL4463346', float
    'CHEMBL4472235' float);
*/


-- 4
create or replace view nananeva.next_and_second_most_similar_target as
select
    source_molecule_reference,
    target_molecule_reference,
    tanimoto_similarity_score,
    lead(target_molecule_reference, 1) over
    	(partition by target_molecule_reference order by tanimoto_similarity_score desc) AS next_most_similar_target,
    nth_value(target_molecule_reference, 2)
    	over (partition by source_molecule_reference order by tanimoto_similarity_score desc rows between unbounded preceding and current row)
    	as second_most_similar_target
from nananeva.dm_top10_fct_molecules_similarities
where source_molecule_reference != target_molecule_reference;


-- 5
create or replace view nananeva.avg_similarity_score_per_groups as
select
    case
        when grouping(fct.source_molecule_reference) = 1 and
        	 grouping(dim.aromatic_rings) = 1 and
        	 grouping(dim.heavy_atoms) = 1
        then 'TOTAL'
        else cast(fct.source_molecule_reference as varchar)
    end as source_molecule_reference,
    case
    	when grouping(fct.source_molecule_reference) = 1 and
        	 grouping(dim.aromatic_rings) = 1 and
        	 grouping(dim.heavy_atoms) = 1
        then 'TOTAL'
        else cast(dim.aromatic_rings as varchar)
    end as source_molecule_aromatic_rings,
    case
    	when grouping(fct.source_molecule_reference) = 1 and
        	 grouping(dim.aromatic_rings) = 1 and
        	 grouping(dim.heavy_atoms) = 1
        then 'TOTAL'
        else cast(dim.heavy_atoms as varchar)
    end as source_molecule_heavy_atoms,
    avg(fct.tanimoto_similarity_score) as avg_tanimoto_similarity_score
from nananeva.dm_top10_fct_molecules_similarities fct
join nananeva.dm_top10_dim_molecules dim
on fct.source_molecule_reference = dim.chembl_id
group by
    grouping sets(
        (fct.source_molecule_reference),
        (dim.aromatic_rings, dim.heavy_atoms),
        (dim.heavy_atoms),
        ()
    );
