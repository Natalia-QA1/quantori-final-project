-- 1
create or replace view nananeva.avg_similarity_score_per_source_molecule as
select source_molecule_reference ,
	   avg(tanimoto_similarity_score) as avg_similarity_score_per_source_molecule
from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024_test
where has_duplicates_of_last_largest_score is false  --only top-10
group by source_molecule_reference ;


-- 2
create or replace view nananeva.avg_alogp_deviation as
with source_alogp as (
    select
        f.source_molecule_reference as source_mol,
        d.alogp as source_alogp
    from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 f
    join nananeva.dm_top10_dim_molecules_data_03_06_2024 d
    on f.source_molecule_reference = d.chembl_id
    group by
        f.source_molecule_reference,
        d.alogp
),
target_alogp as (
    select
        f.target_molecule_reference as target_mol,
        d.alogp as target_alogp
    from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 f
    join nananeva.dm_top10_dim_molecules_data_03_06_2024 d
    on f.target_molecule_reference = d.chembl_id
    group by
        f.target_molecule_reference,
        d.alogp
)
select
    fct.source_molecule_reference,
    avg(abs(tgt.target_alogp - src.source_alogp)) as avg_alogp_deviation
from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 fct
join target_alogp tgt
on  fct.target_molecule_reference = tgt.target_mol
join source_alogp src
on  fct.source_molecule_reference = src.source_mol
where  fct.has_duplicates_of_last_largest_score is false --only top-10
group by  fct.source_molecule_reference;


-- 3
create or replace view nananeva.pivot_10_random_source_molecules as
 select
    target_molecule_reference,
    max(case when source_molecule_reference = 'CHEMBL1521628' then tanimoto_similarity_score end) as 'CHEMBL1521628',
    max(case when source_molecule_reference = 'CHEMBL1521632' then tanimoto_similarity_score end) as 'CHEMBL1521632',
    max(case when source_molecule_reference = 'CHEMBL1521690' then tanimoto_similarity_score end) as 'CHEMBL1521690',
    max(case when source_molecule_reference = 'CHEMBL1521773' then tanimoto_similarity_score end) as 'CHEMBL1521773',
    max(case when source_molecule_reference = 'CHEMBL1521808' then tanimoto_similarity_score end) as 'CHEMBL1521808',
    max(case when source_molecule_reference = 'CHEMBL1521873' then tanimoto_similarity_score end) as 'CHEMBL1521873',
    max(case when source_molecule_reference = 'CHEMBL4462956' then tanimoto_similarity_score end) as 'CHEMBL4462956',
    max(case when source_molecule_reference = 'CHEMBL4463317' then tanimoto_similarity_score end) as 'CHEMBL4463317',
    max(case when source_molecule_reference = 'CHEMBL4463346' then tanimoto_similarity_score end) as 'CHEMBL4463346',
    max(case when source_molecule_reference = 'CHEMBL4472235' then tanimoto_similarity_score end) as 'CHEMBL4472235'
from
    nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024_test
where  source_molecule_reference in
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
from
    nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024_test
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
from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 fct
join nananeva.dm_top10_dim_molecules_data_03_06_2024 dim
on fct.source_molecule_reference = dim.chembl_id
group by
    grouping sets(
        (fct.source_molecule_reference),
        (dim.aromatic_rings, dim.heavy_atoms),
        (dim.heavy_atoms),
        ()
    );
