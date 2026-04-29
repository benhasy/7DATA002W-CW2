-- Create external file format for Parquet

-- Creates external tables over three curated Parquet folders
-- External tables will query the Parquet files directly in blob storage to reduce costs
-- Parquet chosen over CSV as it's columnar - Synapse only reads the columns it needs, not the whole file

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET
);

-- External table 1: Yearly review trends

-- Shows how review volume and average score changed year by year

CREATE EXTERNAL TABLE yearly_review_trends (
    ReviewYear INT,
    TotalReviews BIGINT,
    AvgScore FLOAT,
    UniqueReviewers BIGINT
)
WITH (
    LOCATION = 'yearly_review_trends/',
    DATA_SOURCE = curated_source,
    FILE_FORMAT = parquet_format
);

-- External table 2: Helpfulness by score band

-- Looks at whether positive or negative reviews get rated more useful by other customers
-- Rows where the denominator was zero were set to null in the Spark notebook and filtered out before writing to curated

CREATE EXTERNAL TABLE helpfulness_by_score_band (
    SentimentBand VARCHAR(20),
    Score INT,
    TotalReviews BIGINT,
    AvgHelpfulnessRatio FLOAT
)
WITH (
    LOCATION = 'helpfulness_by_score_band/',
    DATA_SOURCE = curated_source,
    FILE_FORMAT = parquet_format
);

-- External table 3: Top reviewer cohort

-- Reviewers with 10 or more reviews, compared against the overall average score of 4.18 
-- UserId was hashed so no personal data is exposed at the serving layer

CREATE EXTERNAL TABLE top_reviewer_cohort (
    UserId VARCHAR(256),
    ReviewCount BIGINT,
    AvgScore FLOAT,
    VsOverallAvg FLOAT
)
WITH (
    LOCATION = 'top_reviewer_cohort/',
    DATA_SOURCE = curated_source,
    FILE_FORMAT = parquet_format
);