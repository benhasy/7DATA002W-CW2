-- Create external data source pointing to curated container

-- Points Synapse at the curated container in our storage account.
-- Putting the path here means we only need to change it in one place if the storage account ever changes

CREATE EXTERNAL DATA SOURCE curated_source
WITH (
    LOCATION = 'abfss://curated@stcw2bdcbh.dfs.core.windows.net/'
);