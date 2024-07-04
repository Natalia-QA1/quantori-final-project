-- STORAGE LAYER FACT TABLES
CREATE TABLE IF NOT EXISTS nananeva.st_fct_source_molecules_morgan_fingerprints (
    chembl_id VARCHAR(20) NOT NULL,
    morgan_fingerprint BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS nananeva.st_fct_target_molecues_data_03_06_2024_similarities (
    target_chembl_id VARCHAR(20) NOT NULL,
    source_chembl_id VARCHAR(20) NOT NULL,
    similarity_score NUMERIC NOT NULL
);

-- STORAGE LAYER DIMENSION TABLES

CREATE TABLE IF NOT EXISTS nananeva.st_dim_chembl_id_lookup (
    chembl_id VARCHAR(20) NOT NULL UNIQUE,
    entity_type VARCHAR(50),
    last_active NUMERIC,
    resource_url  VARCHAR(100),
    status VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS nananeva.st_dim_molecule_dictionary (
    chembl_id VARCHAR(20) NOT NULL UNIQUE,
    pref_name VARCHAR(255),
    max_phase NUMERIC,
    therapeutic_flag NUMERIC,
    dosed_ingredient NUMERIC,
    structure_type VARCHAR(10),
    chebi_par_id NUMERIC,
    molecule_type VARCHAR(30),
    first_approval NUMERIC,
    oral NUMERIC,
    parenteral NUMERIC,
    topical NUMERIC,
    black_box_warning NUMERIC,
    natural_product NUMERIC,
    first_in_class NUMERIC,
    chirality NUMERIC,
    prodrug NUMERIC,
    inorganic_flag NUMERIC,
    usan_year NUMERIC,
    availability_type NUMERIC,
    usan_stem VARCHAR(50),
    polymer_flag NUMERIC,
    usan_substem VARCHAR(50),
    usan_stem_definition VARCHAR(1000),
    indication_class VARCHAR(1000),
    withdrawn_flag NUMERIC,
    chemical_probe NUMERIC,
    orphan NUMERIC
);

CREATE TABLE IF NOT EXISTS nananeva.st_dim_compound_properties (
    chembl_id VARCHAR(20) NOT NULL UNIQUE,
    mw_freebase NUMERIC,
    alogp NUMERIC,
    hba NUMERIC,
    hbd NUMERIC,
    psa NUMERIC,
    rtb NUMERIC,
    ro3_pass VARCHAR(3),
    num_ro5_violations NUMERIC,
    cx_most_apka NUMERIC,
    cx_most_bpka NUMERIC,
    cx_logp NUMERIC,
    cx_logd NUMERIC,
    molecular_species VARCHAR(50),
    full_mwt NUMERIC,
    aromatic_rings NUMERIC,
    heavy_atoms NUMERIC,
    qed_weighted NUMERIC,
    mw_monoisotopic NUMERIC,
    full_molformula VARCHAR(100),
    hba_lipinski NUMERIC,
    hbd_lipinski NUMERIC,
    num_lipinski_ro5_violations NUMERIC,
    np_likeness_score NUMERIC
);

CREATE TABLE IF NOT EXISTS nananeva.st_dim_compound_structures_1 (
    chembl_id VARCHAR(30) NOT NULL UNIQUE,
    molfile TEXT,
    standard_inchi TEXT,
    standard_inchi_key VARCHAR(1000),
    canonical_smiles VARCHAR(10000)
);

-- DATA MART TABLES

CREATE TABLE IF NOT EXISTS nananeva.dm_top10_dim_molecules_data_03_06_2024 (
    chembl_id VARCHAR(20) NOT NULL,
    molecule_type TEXT,
    mw_freebase NUMERIC,
    alogp NUMERIC,
    psa NUMERIC,
    cx_logp NUMERIC,
    molecular_species VARCHAR(50),
    full_mwt NUMERIC,
    aromatic_rings NUMERIC,
    heavy_atoms NUMERIC
);

CREATE TABLE IF NOT EXISTS nananeva.dm_top10_fct_molecules_similarities_data_03_06_2024 (
    target_molecule_reference VARCHAR(20) NOT NULL,
    source_molecule_reference VARCHAR(20) NOT NULL,
    tanimoto_similarity_score NUMERIC,
    has_duplicates_of_last_largest_score BOOLEAN NOT NULL
);
