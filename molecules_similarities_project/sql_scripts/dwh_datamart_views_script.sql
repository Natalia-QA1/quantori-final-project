-- 7a. Average similarity score per source molecule
create or replace view nananeva.avg_similarity_score_per_source_molecule as
select source_molecule_reference ,
	   avg(tanimoto_similarity_score) as avg_similarity_score_per_source_molecule
from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024_test
where has_duplicates_of_last_largest_score is false  --only top-10
group by source_molecule_reference ;


-- 7b. Average deviation of alogp of similar molecule from the source molecule
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


/*
8a. Choose 10 random source molecules and create a pivot view where first column is the
chembl_id of the target molecule, rest columns are the source molecule’s chembl_id and
each cell is the similarity score of the corresponding source and target molecules.
 */
create or replace view nananeva.pivot_10_random_source_molecules as
 select
    target_molecule_reference,
    max(case when source_molecule_reference = 'CHEMBL1521628' then tanimoto_similarity_score end) as "CHEMBL1521628",
    max(case when source_molecule_reference = 'CHEMBL1521632' then tanimoto_similarity_score end) as "CHEMBL1521632",
    max(case when source_molecule_reference = 'CHEMBL1521690' then tanimoto_similarity_score end) as "CHEMBL1521690",
    max(case when source_molecule_reference = 'CHEMBL1521773' then tanimoto_similarity_score end) as "CHEMBL1521773",
    max(case when source_molecule_reference = 'CHEMBL1521808' then tanimoto_similarity_score end) as "CHEMBL1521808",
    max(case when source_molecule_reference = 'CHEMBL1521873' then tanimoto_similarity_score end) as "CHEMBL1521873",
    max(case when source_molecule_reference = 'CHEMBL4462956' then tanimoto_similarity_score end) as "CHEMBL4462956",
    max(case when source_molecule_reference = 'CHEMBL4463317' then tanimoto_similarity_score end) as "CHEMBL4463317",
    max(case when source_molecule_reference = 'CHEMBL4463346' then tanimoto_similarity_score end) as "CHEMBL4463346",
    max(case when source_molecule_reference = 'CHEMBL4472235' then tanimoto_similarity_score end) as "CHEMBL4472235"
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

select *
from crosstab(
    $$
    select target_molecule_reference, source_molecule_reference, tanimoto_similarity_score
    from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024
    where source_molecule_reference in
    ('chemb6238', 'chemb6344', 'chemb6363', 'chemb6222', 'chemb267864', 'chemb266484', 'chemb268097', 'chemb6214')
    order by 1, 2
    $$
) as ct (
    target_molecule_reference text,
    "chemb6238" float,
    "chemb6344" float,
    "chemb6363" float,
    "chemb6222" float,
    "chemb267864" float,
    "chemb266484" float,
    "chemb268097" float,
    "chemb6214" float
);
*/


/*
 8b. In one query, display all
  -source molecule chembl_id,
  -target molecule chembl_id,
  -similarity score,
  -chembl_id of the next most similar target molecule compared to the target molecule of the current row,
  -chembl_id of the second most similar target_molecule to the current's row source_molecule
 */
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


/*
8c. Average similarity score per:
i. Source_molecule
ii. Source_molecule’s aromatic_rings and source_molecule’s heavy_atoms
iii. Source_molecule’s heavy_atoms
iv. Whole dataset
 */

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
