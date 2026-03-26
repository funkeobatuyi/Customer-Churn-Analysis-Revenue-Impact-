-- Create database and table
CREATE DATABASE saas_churn_analysis;
USE saas_churn_analysis;
CREATE TABLE user_monthly (
user_id VARCHAR(20),
month VARCHAR(20),
tenure_month INT,
plan_type VARCHAR(20),
monthly_price DECIMAL(10,2),
mrr DECIMAL(10,2),
sessions INT,
feature_usage_score INT,
support_tickets INT,
payment_failures INT,
churned INT,
churned_next_month INT,
nps_score INT,
product_incident INT,
active_seats INT
);

CREATE TABLE users (
user_id VARCHAR(20),
signup_date VARCHAR(50) ,
country VARCHAR(50),
company_size VARCHAR(50),
plan_type VARCHAR(50),
monthly_price DECIMAL(10,2),
sessions_per_month INT,
feature_usage_score INT,
support_tickets INT,
payment_failures INT,
last_active_date DATE,
churned INT,
churn_date VARCHAR(20),
tenure_months INT,
total_revenue DECIMAL(12,2),
ltv_12m INT,
industry VARCHAR(100),
acquisition_channel VARCHAR(100),
billing_period VARCHAR(20),
seats_purchased INT,
discount_pct DECIMAL(5,2),
payment_method VARCHAR(50)
);

-- used load data local infile to import data

-- IMPORT USER_MONTHLY DATA
   TRUNCATE TABLE user_monthly;

LOAD DATA LOCAL INFILE 'C:/Users/USER/Downloads/usermonthlyfix.csv'
INTO TABLE user_monthly
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS 
(
user_id, 
  @var_month, -- We capture the Excel date here
  tenure_month, 
  plan_type, 
  monthly_price, 
  mrr, 
  sessions, 
  feature_usage_score, 
  support_tickets, 
  payment_failures, 
  churned, 
  churned_next_month, 
  nps_score, 
  product_incident, 
  active_seats
)
SET month = STR_TO_DATE (TRIM(@var_month), '%d/%m/%Y')
;

-- IMPORT USER DATA
TRUNCATE users;

-- Churn_date has two data arrangement Here's the fix
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
  user_id, 
  @signup_date, 
  country, 
  company_size, 
  plan_type, 
  monthly_price, 
  sessions_per_month, 
  feature_usage_score, 
  support_tickets, 
  payment_failures, 
  @last_active_date, 
  churned, 
  @churn_date, 
  tenure_months, 
  total_revenue, 
  ltv_12m, 
  industry, 
  acquisition_channel, 
  billing_period, 
  seats_purchased, 
  discount_pct, 
  payment_method
)
SET 
    signup_date = STR_TO_DATE(@signup_date, '%m/%d/%Y'),
    last_active_date = STR_TO_DATE(@last_active_date, '%m/%d/%Y'),
    -- This CASE statement prevents the "Incorrect date value" error
    churn_date = CASE 
        WHEN @churn_date = '\\N' OR @churn_date = '' OR @churn_date IS NULL THEN NULL 
        ELSE STR_TO_DATE(@churn_date, '%m/%d/%Y') 
    END;

SHOW VARIABLES LIKE 'secure_file_priv';



-- Confirm table size
SELECT* FROM user_monthly;
SELECT COUNT(*)
 FROM user_monthly;
SELECT * FROM users;
SELECT COUNT(*) 
FROM users;

-- Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize formats
-- 3. Check for nulls/missing values

-- Checking for duplicates
SELECT user_id, COUNT(*)
FROM users
GROUP BY user_id
HAVING COUNT(*) > 1;

SELECT *,
COUNT(*)
FROM user_monthly
GROUP BY user_id, month, tenure_month, plan_type, monthly_price, mrr,
sessions, feature_usage_score, support_tickets, payment_failures,
churned, churned_next_month, nps_score, product_incident, active_seats
HAVING COUNT(*) > 1;


-- checking for correct spellings 
SELECT plan_type 
FROM user_monthly
GROUP BY plan_type;

SELECT industry
FROM users
GROUP BY industry;

SELECT acquisition_channel
FROM users
GROUP BY acquisition_channel;

SELECT billing_period
FROM users
GROUP BY billing_period;

SELECT payment_method
FROM users
GROUP BY payment_method;


-- Checking for blanks and Null
SELECT*
FROM user_monthly
WHERE user_id = ' ' OR NULL;


DESCRIBE users;

SELECT * FROM saas_churn_analysis.users;

SELECT last_active_date
FROM saas_churn_analysis.users
ORDER BY last_active_date ASC
LIMIT 10;
-- CHURN RATE 

SELECT COUNT(*)
FROM saas_churn_analysis.users;

SELECT COUNT(*) AS Tot_churned
 FROM saas_churn_analysis.users
 WHERE Churned = 1;
 
 -- CHURN RATE Calculation
 SELECT
      COUNT(CASE WHEN churned = 1 THEN 1 END) AS Total_churned,
      COUNT(*) AS total_customers,
      100.0 * COUNT(CASE WHEN churned = 1 THEN 1 END) / COUNT(*) AS Churn_rate_percent
FROM saas_churn_analysis.users;

-- CHURN BY SUBSCRIPTION
SELECT plan_type,
COUNT(CASE WHEN churned = 1 THEN 1 END) AS Total_churned,
      COUNT(*) AS total_customers,
      100.0 * COUNT(CASE WHEN churned = 1 THEN 1 END) / COUNT(*) AS Churn_rate_percent
FROM saas_churn_analysis.users
GROUP BY plan_type;

 -- Average monthly subscription price per plan_type
SELECT plan_type, AVG(monthly_price)
 FROM saas_churn_analysis.users
 GROUP BY plan_type;
 
  -- AVERAGE CUSTOMER TENURE BEFORE CHURN
SELECT plan_type,  
AVG(tenure_months)
FROM saas_churn_analysis.users
WHERE churned = 1
GROUP BY plan_type
 ;

SELECT plan_type, 
100.0 * SUM(churned) / Count(*) AS churn_rate,
AVG(CASE WHEN churned = 1 THEN tenure_months END) AS Avg_tenure
FROM saas_churn_analysis.users
GROUP BY plan_type
ORDER BY Avg_tenure DESC;

-- Age tenure distribution
SELECT * FROM user_monthly;
SELECT * FROM users;
SELECT 
	CASE
		WHEN tenure_months <= 6 THEN 'New 0-6 months'
        WHEN tenure_months <= 12 THEN 'Mid 7-12months'
        ELSE 'Established 13+ months'
        END AS user_age_segment,
	COUNT(*) AS user_count,
    ROUND(AVG(churned)*100.0, 2) AS Churn_rate_percent
 FROM saas_churn_analysis.users
 GROUP BY user_age_segment
 ORDER BY MIN(tenure_months);
 
 -- Regional Distribution
 SELECT country,
	Count(*) AS total_users,
	ROUND(AVG(churned)*100, 2) AS churn_rate_percent
	FROM saas_churn_analysis.users
	GROUP BY country
	ORDER BY total_users DESC
	LIMIT 10;
-- Most subscribed plan
 SELECT plan_type,
	Count(*) AS total_subscribers,
	ROUND(AVG(churned)*100, 2) AS churn_rate_percent
	FROM saas_churn_analysis.users
	GROUP BY plan_type
	ORDER BY total_subscribers DESC;
    
    



