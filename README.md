# MOLECULE SIMILARITIES PROJECT

## Project description

Terms and abbreviations:
* DWH - data warehouse
* DM - data mart
* Target molecules - required selected molecules downloaded from s3 bucket
* Source molecules - all molecules downloaded from chembl database
* Tables prefix:
   - st_: storage layer
   - dm_: datamart layer
   - _fct_: fact table
   - _dim_: dimension table


### Data Warehouse Architecture
The Data Warehouse (DWH) for this project follows a two-layer architecture, comprising the Storage Layer and the Representation Layer (Datamart). 
The decision to omit a staging layer is based on the direct access to the ChEMBL database via API or a new client, aiming to save space and reduce potential costs.

#### Storage Layer
The Storage Layer is designed with a complex star schema, featuring several fact tables and dimension tables. 
It consists of the following tables:

Dimension Tables:
- st_dim_chembl_id_lookup
- st_dim_molecule_dictionary
- st_dim_compound_properties
- st_dim_compound_structures

Fact Tables:
- st_fct_source_molecules_morgan_fingerprints: Stores computed Morgan fingerprints for each molecule. This table can be extended to include other computations in the future.
- st_fct_target_molecules_data_03_06_2024_similarities: Stores computed similarity scores for target molecules compared to all other source molecules.


#### Representation Layer (Datamart)
The Representation Layer is a datamart designed to identify the top-10 most similar source molecules for target molecules. 
It consists of the following tables:
![datamart_tables](https://github.com/Natalia-QA1/quantori-final-project/blob/main/assets/datamart_tables_erd.PNG)

Fact Table:
- dm_top10_fct_molecules_similarities_data_03_06_2024: Stores information about the top 10 similarities for each target molecule.

Dimension Table:
- dm_top10_dim_molecules_data_03_06_2024: Stores descriptive information only about molecules included in the datamart (both target and source molecules).

This structured approach ensures that the DWH effectively supports the goal of finding the most similar molecules, while maintaining efficient storage and cost management.
