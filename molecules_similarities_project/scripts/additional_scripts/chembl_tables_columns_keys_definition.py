"""
Script defines tables columns keys to extract from api
for each particular table.
"""

compound_properties_keys_to_extract = [
    "molecule_chembl_id",
    ("molecule_properties.mw_freebase", "mw_freebase"),
    ("molecule_properties.alogp", "alogp"),
    ("molecule_properties.hba", "hba"),
    ("molecule_properties.hbd", "hbd"),
    ("molecule_properties.psa", "psa"),
    ("molecule_properties.rtb", "rtb"),
    ("molecule_properties.ro3_pass", "ro3_pass"),
    ("molecule_properties.num_ro5_violations", "num_ro5_violations"),
    ("molecule_properties.cx_most_apka", "cx_most_apka"),
    ("molecule_properties.cx_most_bpka", "cx_most_bpka"),
    ("molecule_properties.cx_logp", "cx_logp"),
    ("molecule_properties.cx_logd", "cx_logd"),
    ("molecule_properties.molecular_species", "molecular_species"),
    ("molecule_properties.full_mwt", "full_mwt"),
    ("molecule_properties.aromatic_rings", "aromatic_rings"),
    ("molecule_properties.heavy_atoms.qed_weighted", "heavy_atoms"),
    ("molecule_properties.qed_weighted", "qed_weighted"),
    ("molecule_properties.mw_monoisotopic", "mw_monoisotopic"),
    ("molecule_properties.full_molformula", "full_molformula"),
    ("molecule_properties.hba_lipinski", "hba_lipinski"),
    ("molecule_properties.hbd_lipinski", "hbd_lipinski"),
    ("molecule_properties.num_lipinski_ro5_violations", "num_lipinski_ro5_violations"),
    ("molecule_properties.np_likeness_score", "np_likeness_score")
]

compound_properties_columns = {
    "chembl_id": "str",
    "mw_freebase": "float",
    "alogp": "float",
    "hba": "float",
    "hbd": "float",
    "psa": "float",
    "rtb": "float",
    "ro3_pass": "float",
    "num_ro5_violations": "float",
    "cx_most_apka": "float",
    "cx_most_bpka": "float",
    "cx_logp": "float",
    "cx_logd": "float",
    "molecular_species": "str",
    "full_mwt": "float",
    "aromatic_rings": "float",
    "heavy_atoms": "float",
    "qed_weighted": "float",
    "mw_monoisotopic": "float",
    "full_molformula": "str",
    "hba_lipinski": "float",
    "hbd_lipinski": "float",
    "num_lipinski_ro5_violations": "float",
    "np_likeness_score": "float"
}


compounds_structures_keys_to_extract = [
    "molecule_chembl_id",
    ("molecule_structures.molfile", "molfile"),
    ("molecule_structures.standard_inchi", "standard_inchi"),
    ("molecule_structures.standard_inchi_key", "standard_inchi_key"),
    ("molecule_structures.canonical_smiles", "canonical_smiles")
    ]

compounds_structures_table_columns = {
    "chembl_id": "str",
    "molfile": "str",
    "standard_inchi": "str",
    "standard_inchi_key": "str",
    "canonical_smiles": "str"
}


lookup_ids_keys_to_extract = [
    "chembl_id",
    "entity_type",
    "last_active",
    "resource_url",
    "status"
]

lookup_ids_table_columns = {
    "chembl_id": "str",
    "entity_type": "str",
    "last_active": "float",
    "resource_url": "str",
    "status": "str"
}


molecule_dictionary_keys_to_extract = [
    "molecule_chembl_id",
    "pref_name",
    "max_phase",
    "therapeutic_flag",
    "dosed_ingredient",
    "structure_type",
    "chebi_par_id",
    "molecule_type",
    "first_approval",
    "oral",
    "parenteral",
    "topical",
    "black_box_warning",
    "natural_product",
    "first_in_class",
    "chirality",
    "prodrug",
    "inorganic_flag",
    "usan_year",
    "availability_type",
    "usan_stem",
    "polymer_flag",
    "usan_substem",
    "usan_stem_definition",
    "indication_class",
    "withdrawn_flag",
    "chemical_probe",
    "orphan"
]

molecule_dictionary_table_columns = {
    "molecule_chembl_id": "str",
    "pref_name": "str",
    "max_phase": "float",
    "therapeutic_flag": "float",
    "dosed_ingredient": "float",
    "structure_type": "str",
    "chebi_par_id": "float",
    "molecule_type": "str",
    "first_approval": "float",
    "oral": "float",
    "parenteral": "float",
    "topical": "float",
    "black_box_warning": "float",
    "natural_product": "float",
    "first_in_class": "float",
    "chirality": "float",
    "prodrug": "float",
    "inorganic_flag": "float",
    "usan_year": "float",
    "availability_type": "float",
    "usan_stem": "str",
    "polymer_flag": "float",
    "usan_substem": "str",
    "usan_stem_definition": "str",
    "indication_class": "str",
    "withdrawn_flag": "float",
    "chemical_probe": "float",
    "orphan": "float"
}
