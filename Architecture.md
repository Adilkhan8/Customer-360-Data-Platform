                    Daily CSV Files

         customers.csv
         orders.csv
         website_visits.csv
         support_tickets.csv

                    ↓

              Amazon S3

     s3://customer360/raw/

                    ↓

         AWS Glue Crawlers

                    ↓

        AWS Glue Data Catalog

                    ↓

        AWS Glue PySpark Jobs

        - Data Cleaning
        - Standardization
        - Deduplication
        - Incremental Loads
        - Audit Logging

                    ↓

        Curated Zone in S3

     s3://customer360/curated/

                    ↓

          Amazon Redshift

         Star Schema

                    ↓

        Analytical Views

    - Customer Lifetime Value
    - Churn Risk
    - Purchase Patterns
