# DATA 226 HW 3.2 - E-commerce Product Catalog
Complete rubric-oriented implementation using Airflow, dbt, Snowflake, SCD Type 2 snapshots, tests, and zero-copy cloning.

## Important
Cloud evidence must be produced in the student's own Snowflake/Airflow environment. Do not submit placeholder evidence as executed output.

## Run order
1. Execute sql/01_setup_snowflake.sql.
2. Configure Snowflake credentials and Airflow connection `snowflake_conn`.
3. Put products.csv at the DAG CSV_PATH or adjust the path.
4. Run `dbt debug`, then the Airflow DAG.
5. Save first-run evidence. Replace products.csv with products_changed.csv and rerun to demonstrate SCD2.
6. Execute cloning statements in sql/02_zero_copy_cloning.sql and save evidence.
7. Use sql/03_verification.sql for verification screenshots.
