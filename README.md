# MOLECULE SIMILARITIES PROJECT

# Project description

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


## Data Warehouse Architecture
The Data Warehouse (DWH) for this project follows a two-layer architecture, comprising the Storage Layer and the Representation Layer (Datamart). 
The decision to omit a staging layer is based on the direct access to the ChEMBL database via API or a new client, aiming to save space and reduce potential costs.

#### Storage Layer
The Storage Layer is designed as a star schema, featuring fact table and dimension tables. 
It consists of the following tables:
Dimension Tables:
- st_dim_chembl_id_lookup
- st_dim_molecule_dictionary
- st_dim_compound_properties
- st_dim_compound_structures

Fact Table:
- st_fct_source_molecules_morgan_fingerprints: Stores computed Morgan fingerprints for each molecule. This table can be extended to include other computations in the future.
  
###### Firstly, I planned to create a fact table with similarities in DWH (st_fct_target_molecules_similarities: Stores computed similarity scores for target molecules compared to all other source molecules), but the I decided to delete it since we will store all similarities in parquet in s3 which is more efficient strategy because we have a huge amount of data. It can be the case, for example, if we want our end users who work with data to have direct fast access to data (instead connecting to s3 and retrieve data). But also here we have to take into consideration available resources (available storage, cost computations, etc).



#### Representation Layer (Datamart)
The Representation Layer is a datamart designed to identify the top-10 most similar source molecules for target molecules. 
It consists of the following tables:
Fact Table:
- dm_top10_fct_molecules_similarities: Stores information about the top 10 similarities for each target molecule.
Dimension Table:
- dm_top10_dim_molecules: Stores descriptive information only about molecules included in the datamart (both target and source molecules).



## Project launch
Steps:
1. Create DWH tables in Postgres database:
   In the folder "molecules_similarities_project/sql_scripts" run "dwh_tables_definition_ddl_script.sql" script.
   This will create tables for both layers: storage and datamart.
2. ChemBL tables data loading pipeline
   In the folder "molecules_similarities_project/scripts/run_scripts" run the following scripts:
   - chembl_tables_pipeline_run_compound_properties_table.py
   - chembl_tables_pipeline_run_compound_structures_table.py
   - chembl_tables_pipeline_run_script_lookup_table.py
   - chembl_tables_pipeline_run_script_molecule_dictionary_table.py
   This will fetch data from ChemBL web service and load them into dimension tables on the storage layer: 
   "st_dim_chembl_id_lookup", "st_dim_molecule_dictionary", "st_dim_compound_properties", "st_dim_compound_structures".
4. Source molecules fingerprints computations pipeline
   In the folder "molecules_similarities_project/scripts/run_scripts" run "fingerprints_computation_pipeline_run_script.py"    
   script.
   This will fetch "chembl_id, canonical_smiles" of all source molecules from DWH "st_dim_compound_structures" table, then 
   calculate fingerprints for each molecule, load this data into "st_fct_source_molecules_morgan_fingerprints" on the 
   storage layer, save data into several parquet files and put them into s3 bucket.
5. Target molecules similarities computations pipeline
   In the folder "molecules_similarities_project/scripts/run_scripts" run "similarities_computation_run_script.py" script.
   This will compute Tanimoto similarity scores for each target molecule from scv files in s3 bucket with all source molecules from ChemBL database.
   For each target molecule the full similarity score table will be saved into a separate parquet file and upload to S3 bucket.
6. Top-10 molecules similarities computation pipeline
   - In the folder "molecules_similarities_project/scripts/run_scripts" run "top_10_similarities_computation_run_script.py" script.
     It will load data computes after fetching and processing parquet files stored in s3 into fact table "dm_top10_fct_molecules_similarities"
   - In the folder "molecules_similarities_project/sql_scripts" in the "dwh_datamart_tables_data_load_pipepline_script.sql" script trigger 
     "insert_data_dm_top10_fct()" procedure. This will select chembl molecules ids from the fact table and insert descriptive data only for presented molecules.
7. Create DM views
   In the folder "molecules_similarities_project/sql_scripts" run "dwh_datamart_views_script.sql" script.

   



## Project results

### Airflow

1. Input files Sensor DAG:
   ![sensor](https://github.com/Natalia-QA1/quantori-final-project/blob/main/screenshots/Input_files_s3_sensor_dag_1.PNG)
   
