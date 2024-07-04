create or replace procedure nananeva.insert_data_dm_top10_fct()
language plpgsql
as $$
begin
	with ranked_similarities as (
    select
        target_chembl_id,
        source_chembl_id,
        similarity_score,
        row_number() over (partition by target_chembl_id order by similarity_score desc) as row_num,
        dense_rank() over (partition by target_chembl_id order by similarity_score desc) as dense_rnk
    from nananeva.st_fct_target_molecules_data_03_06_2024_similarities
    where source_chembl_id != target_chembl_id
	),
	top_10_similarities as (
	    select
	        target_chembl_id,
	        source_chembl_id,
	        similarity_score,
	        row_num,
	        dense_rnk,
	        case when row_num = 10 then similarity_score else null
	        end as tenth_similarity_score
	    from ranked_similarities
	),
	tenth_similarity as (
	    select distinct
	        target_chembl_id,
	        similarity_score as tenth_similarity_score
	    from top_10_similarities
	    where row_num = 10
	),
	top_10_with_duplicates as (
	    select
	        rs.target_chembl_id,
	        rs.source_chembl_id,
	        rs.similarity_score,
	        case when rs.row_num > 10 and rs.similarity_score = ts.tenth_similarity_score then true else false
	        end as has_duplicates_of_last_largest_score
	    from ranked_similarities rs
	    left join tenth_similarity ts
	    on rs.target_chembl_id = ts.target_chembl_id
	       and rs.similarity_score = ts.tenth_similarity_score
	    where rs.row_num <= 10 or (rs.row_num > 10 and rs.similarity_score = ts.tenth_similarity_score)
	)
	insert into nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 (
	target_molecule_reference,
	source_molecule_reference,
	tanimoto_similarity_score,
	has_duplicates_of_last_largest_score
	)
	select
	    target_chembl_id,
	    source_chembl_id,
	    similarity_score,
	    has_duplicates_of_last_largest_score
	from top_10_with_duplicates
	;



end
$$


create or replace procedure nananeva.insert_data_dm_top10_dim()
language plpgsql
as $$
begin
	with data_mart_chembl_ids as(
	select distinct(target_molecule_reference) as chembl_id
	from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024
	union all
	select distinct(source_molecule_reference) as chembl_id
	from nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024
	)
	insert into nananeva.dm_top10_dim_molecules_data_03_06_2024 (
		chembl_id ,
	    molecule_type ,
	    mw_freebase ,
	    alogp ,
	    psa ,
	    cx_logp,
	    molecular_species ,
	    full_mwt ,
	    aromatic_rings,
	    heavy_atoms)
	select
		md.chembl_id,
		md.molecule_type ,
		mp.mw_freebase ,
		mp.alogp ,
		mp.psa ,
		mp.cx_logp ,
		mp.molecular_species ,
		mp.full_mwt ,
		mp.aromatic_rings ,
		mp.heavy_atoms
	from nananeva.st_dim_molecule_dictionary md
	join nananeva.st_dim_compound_properties mp
	on md.chembl_id = mp.chembl_id
	where md.chembl_id in (select chembl_id from data_mart_chembl_ids);

end
$$


create or replace procedure nananeva.insert_data_dm_top10_pipeline_main()
language plpgsql
as $$
begin
call nananeva.insert_data_dm_top10_fct();
call nananeva.insert_data_dm_top10_dim();

end
$$


call nananeva.insert_data_dm_top10_pipeline_main();