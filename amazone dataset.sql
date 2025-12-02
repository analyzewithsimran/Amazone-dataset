CREATE DATABASE Amazone_Dataset;
USE Amazone_Dataset;

-- BASIC LEVEL 
-- 1. Basic Data Understanding
-- 1. Write a query to list all unique Report_Section values
SELECT DISTINCT
    ï»¿Report_Section
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned;
    
-- 2. Find all unique Dimension categories under CATEGORY_PERFORMANCE.
SELECT 
    Dimension, ï»¿Report_Section
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    ï»¿Report_Section = '\'RETURNS_BY_CATEGORY';

-- 3. Show the top 10 records from the dataset.
SELECT 
    *
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
LIMIT 10;

-- ⭐ INTERMEDIATE LEVEL 
-- 2. Filtering & Sorting
-- 1. Get all records where Metric1 is greater than 100,000,000.
SELECT 
    Metric1
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    Metric1 > 100000000;

-- 2. Show records where Metric3 is NOT NULL.
SELECT 
    Metric3
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    Metric3 IS NOT NULL;
    
-- 3. List all categories (Dimension) under CATEGORY_PERFORMANCE sorted by Metric2 in descending order.
SELECT 
    Dimension, Metric2
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
ORDER BY Metric2 DESC;


-- Analytical level 
-- 3. Aggregations
-- 1. Calculate the total Metric1 for each Report_Section.
SELECT 
    ï»¿Report_Section AS Report_Section,
    SUM(Metric1) AS Total_Metric
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
GROUP BY Report_Section
ORDER BY Total_Metric;

-- 2. Find the average Metric2 for each CATEGORY_PERFORMANCE.
SELECT 
    ï»¿Report_Section AS Report_Section,
    AVG(Metric2) AS Avg_Metric
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
GROUP BY Report_Section
ORDER BY Avg_Metric DESC;

-- 3. Show the max Metric3 value under AVERAGE_ORDER_VALUE section.
SELECT 
    MAX(Metric3) AS Max_Matric,
    ï»¿Report_Section AS Report_Section
FROM
    amazone_dataset.amazon_sales_2025_inr_cleaned
WHERE
    ï»¿Report_Section = 'AVERAGE_ORDER_VALUE';

-- Advanced SQL (Perfect for Resume Projects)
-- 4. Window Functions
-- 1.For each Report_Section, rank the Dimensions by Metric1 (highest first).
SELECT
    ï»¿Report_Section,
    Dimension,
    Metric1,
    RANK() OVER (
        PARTITION BY ï»¿Report_Section
        ORDER BY Metric1 DESC
    ) AS metric1_rank
FROM amazone_dataset.amazon_sales_2025_inr_cleaned;

-- 2. Create a running total of Metric1 for CATEGORY_PERFORMANCE.
SELECT
    ï»¿Report_Section,
    Dimension,
    Metric1,
    SUM(Metric1) OVER (
        PARTITION BY ï»¿Report_Section
        ORDER BY Metric1 DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_metric1
FROM amazon_sales_2025_INR_cleaned
WHERE ï»¿Report_Section = 'RETURNS_BY_CATEGORY';

-- 3. For each Report_Section, calculate % contribution of each Dimension’s Metric1 to the total Metric1.
SELECT
    ï»¿Report_Section,
    Dimension,
    Metric1,
    ROUND(
        Metric1 * 100.0 /
        SUM(Metric1) OVER (PARTITION BY ï»¿Report_Section),
        2
    ) AS metric1_percentage_contribution
FROM amazon_sales_2025_INR_cleaned;

-- CTEs, Subqueries & Self-Joins
-- 5. CTE for Filtered Section
-- 1. Create a CTE to calculate the average Metric1, then return only the rows where Metric1 is greater than this average.
WITH avg_metric AS (
    SELECT AVG(Metric1) AS avg_metric1
    FROM amazon_sales_2025_INR_cleaned
)
SELECT
    t.ï»¿Report_Section,
    t.Dimension,
    t.Metric1,
    t.Metric2,
    t.Metric3,
    t.Metric4
FROM amazon_sales_2025_INR_cleaned AS t
CROSS JOIN avg_metric
WHERE t.Metric1 > avg_metric.avg_metric1;

-- 2. Create a CTE to select the first 5 rows from the dataset.
WITH First_Five AS (SELECT * FROM 
amazon_sales_2025_INR_cleaned 
LIMIT 5)
SELECT * FROM  First_Five;

-- 3. Create a CTE that calculates, for each Report_Section:
-- total Metric1
-- total Metric2
-- Then return the summary results in the main query.
WITH Section_Summary AS (SELECT ï»¿Report_Section AS Report_Section, 
SUM(Metric1) AS total_Metric1, SUM(Metric2) AS total_Metric2 
FROM amazon_sales_2025_INR_cleaned 
GROUP BY Report_Section) 
SELECT * FROM  Section_Summary;

-- 6. Subquery Questions 
-- 1. Find all rows where Metric1 is greater than the overall average Metric1
SELECT 
    Metric1
FROM
    amazon_sales_2025_INR_cleaned
WHERE
    Metric1 > (SELECT 
            AVG(Metric1)
        FROM
            amazon_sales_2025_INR_cleaned);
            
-- 2. Find Dimensions whose Metric1 is equal to the maximum Metric1 in the entire dataset
SELECT 
    Dimension, Metric1
FROM
    amazon_sales_2025_INR_cleaned
WHERE
    Metric1 = (SELECT 
            MAX(Metric1)
        FROM
            amazon_sales_2025_INR_cleaned);
            
-- 3. Show all Dimensions under CATEGORY_PERFORMANCE whose Metric2 is higher than the average Metric2 of CATEGORY_PERFORMANCE
SELECT
    Dimension,
    Metric2
FROM amazon_sales_2025_INR_cleaned
WHERE  ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
  AND Metric2 > (
        SELECT AVG(Metric2)
        FROM amazon_sales_2025_INR_cleaned
        WHERE  ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
    );

-- 7. Business Insights Queries
-- Real-World Analytical Questions
-- 1. Which product category (Dimension) performs the best overall in terms of Metric1?
SELECT
    Dimension,
    Metric1
FROM amazon_sales_2025_INR_cleaned
WHERE Metric1 = (
        SELECT MAX(Metric1)
        FROM amazon_sales_2025_INR_cleaned
    );

-- 2. Which category has the lowest Metric2 under CATEGORY_PERFORMANCE?
SELECT
    Dimension,
    Metric2
FROM amazon_sales_2025_INR_cleaned
WHERE ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
  AND Metric2 = (
        SELECT MIN(Metric2)
        FROM amazon_sales_2025_INR_cleaned
        WHERE ï»¿Report_Section = 'RETURNS_BY_CATEGORY'
    );
    
-- 3. List the categories where the Metric1-to-Metric3 ratio is highest.
SELECT
    Dimension,
    Metric1,
    Metric3,
    (Metric1 / Metric3) AS ratio
FROM amazon_sales_2025_INR_cleaned
WHERE Metric3 IS NOT NULL
  AND (Metric1 / Metric3) = (
        SELECT MAX(Metric1 / Metric3)
        FROM amazon_sales_2025_INR_cleaned
        WHERE Metric3 IS NOT NULL
    );


-- 📊 8. Pivot / Matrix-Style Questions (DB-specific)
-- Pivot Report_Section as Columns
-- 1. Convert rows into columns so that each Report_Section becomes a column and the values show Metric1 for each Dimension (PIVOT or conditional aggregation).
SELECT
    Dimension,
    SUM(CASE WHEN ï»¿Report_Section = 'RETURNS_BY_CATEGORY' THEN Metric1 END) AS category_performance,
    SUM(CASE WHEN ï»¿Report_Section = 'AVERAGE_ORDER_VALUE' THEN Metric1 END) AS average_order_value,
    SUM(CASE WHEN ï»¿Report_Section= 'OVERALL_STATISTICS' THEN Metric1 END) AS overall_statistics
FROM amazon_sales_2025_INR_cleaned
GROUP BY Dimension;

-- 2. Metric Comparison in Single Row (Per Dimension)
-- For each Dimension, show:
-- total Metric1
-- total Metric2
-- total Metric3
-- ratio Metric1 : Metric2
-- ratio Metric1 : Metric3

SELECT
    Dimension,
    SUM(Metric1) AS total_metric1,
    SUM(Metric2) AS total_metric2,
    SUM(Metric3) AS total_metric3,
    ROUND(SUM(Metric1) / NULLIF(SUM(Metric2), 0), 2) AS ratio_metric1_to_metric2,
    ROUND(SUM(Metric1) / NULLIF(SUM(Metric3), 0), 2) AS ratio_metric1_to_metric3
FROM amazon_sales_2025_INR_cleaned
GROUP BY Dimension;

-- CREATE VIEW BASED ON 
-- View will contain:
-- Report_Section
-- Dimension
-- Metric1
-- Total Metric1 of that section
-- Percentage contribution 
CREATE VIEW section_metric1_percent_view AS
SELECT
    ï»¿Report_Section,
    Dimension,
    Metric1,
    SUM(Metric1) OVER (PARTITION BY ï»¿Report_Section) AS section_total_metric1,
    
    ROUND(
        Metric1 * 100.0 /
        SUM(Metric1) OVER (PARTITION BY ï»¿Report_Section),
        2
    ) AS percent_contribution
FROM amazon_sales_2025_INR_cleaned;

SELECT * FROM section_metric1_percent_view;


