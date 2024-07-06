
create or replace procedure nananeva.insert_data_dm_top10_dim_molecules()
language plpgsql
as $$
begin
	with data_mart_chembl_ids as(
	select distinct(target_molecule_reference) as chembl_id
	from nananeva.dm_top10_fct_molecules_similarities
	union all
	select distinct(source_molecule_reference) as chembl_id
	from nananeva.dm_top10_fct_molecules_similarities
	)
	insert into nananeva.dm_top10_dim_molecules(
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
	where md.chembl_id in (select chembl_id from data_mart_chembl_ids)
		  and not exists(select 1 from nananeva.dm_top10_dim_molecules);

end
$$

call nananeva.insert_data_dm_top10_dim_molecules();
