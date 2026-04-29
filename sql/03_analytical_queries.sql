-- Three analytical queries over the curated external tables
-- These run directly against the Parquet files in blob storage via Synapse Serverless SQL

-- Query 1: Yearly review trends

-- Shows how review volume and average score changed over time
-- Useful for understanding platform growth and whether sentiment shifted when the dataset scaled

SELECT 
    ReviewYear,
    TotalReviews,
    AvgScore,
    UniqueReviewers
FROM yearly_review_trends
ORDER BY ReviewYear;

-- Query 2: Helpfulness rate by score band

-- Compares how useful customers find positive vs negative reviews
-- Higher scores consistently attract higher helpfulness ratings, suggesting positivity bias in voting

SELECT 
    SentimentBand,
    Score,
    TotalReviews,
    AvgHelpfulnessRatio
FROM helpfulness_by_score_band
ORDER BY Score;

-- Query 3: Top reviewer cohort vs overall average

-- Identifies reviewers with 10+ reviews and compares their average score against the dataset average (4.18)
-- Identifies if heavy users rate differently to casual ones

SELECT 
    UserId,
    ReviewCount,
    AvgScore,
    VsOverallAvg
FROM top_reviewer_cohort
ORDER BY ReviewCount DESC;