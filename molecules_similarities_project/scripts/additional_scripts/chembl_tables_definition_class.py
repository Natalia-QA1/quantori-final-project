from sqlalchemy import (
    Column,
    String,
    Text,
    Float,
    Numeric,
    LargeBinary
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


"""
The provided code defines SQLAlchemy ORM models for various tables in a 
PostgreSQL database. These models are classes that map to database tables, 
allowing for object-relational mapping (ORM). Each class represents a table, 
and each attribute of the class represents a column in the table.
"""


class MoleculesDictionary(Base):
    __tablename__ = "st_dim_molecule_dictionary"
    chembl_id = Column(
        String(20),
        primary_key=True
    )
    pref_name = Column(String(255))
    max_phase = Column(Float)
    therapeutic_flag = Column(Float)
    dosed_ingredient = Column(Float)
    structure_type = Column(String(10))
    chebi_par_id = Column(Float)
    molecule_type = Column(String(30))
    first_approval = Column(Float)
    oral = Column(Float)
    parenteral = Column(Float)
    topical = Column(Float)
    black_box_warning = Column(Float)
    natural_product = Column(Float)
    first_in_class = Column(Float)
    chirality = Column(Float)
    prodrug = Column(Float)
    inorganic_flag = Column(Float)
    usan_year = Column(Float)
    availability_type = Column(Float)
    usan_stem = Column(String(50))
    indication_class = Column(String(1000))
    withdrawn_flag = Column(Float)
    chemical_probe = Column(Float)
    orphan = Column(Float)


class CompoundStructures(Base):
    __tablename__ = "st_dim_compound_structures"
    chembl_id = Column(
        String(20),
        primary_key=True
    )
    molfile = Column(Text)
    standard_inchi = Column(Text)
    standard_inchi_key = Column(String(1000))
    canonical_smiles = Column(String(10000))


class CompoundProperties(Base):
    __tablename__ = "st_dim_compound_properties"
    chembl_id = Column(
        String(20),
        primary_key=True
    )
    mw_freebase = Column(Float)
    alogp = Column(Float)
    hba = Column(Float)
    hbd = Column(Float)
    psa = Column(Float)
    rtb = Column(Float)
    ro3_pass = Column(String(3))
    num_ro5_violations = Column(Float)
    cx_most_apka = Column(Float)
    cx_most_bpka = Column(Float)
    cx_logp = Column(Float)
    cx_logd = Column(Float)
    molecular_species = Column(String(50))
    full_mwt = Column(Float)
    aromatic_rings = Column(Float)
    heavy_atoms = Column(Float)
    qed_weighted = Column(Float)
    mw_monoisotopic = Column(Float)
    full_molformula = Column(String(100))
    hba_lipinski = Column(Float)
    hbd_lipinski = Column(Float)
    num_lipinski_ro5_violations = Column(Float)
    np_likeness_score = Column(Float)


class ChemblLookupIds(Base):
    __tablename__ = "st_dim_chembl_id_lookup"
    chembl_id = Column(
        String(20),
        primary_key=True
    )
    entity_type = Column(String(50))
    last_active = Column(Float)
    resource_url = Column(String(100))
    status = Column(String(10))


class MorganFingerprint(Base):
    __tablename__ = "st_fct_source_molecules_morgan_fingerprints"
    chembl_id = Column(String(20), primary_key=True)
    morgan_fingerprint = Column(LargeBinary, nullable=False)
