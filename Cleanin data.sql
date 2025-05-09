-- data cleaning

select* 
from layoffs;

-- 1. Remove duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove Any Columns

CREATE TABLE not_layoffs
like layoffs;

select*
from not_layoffs;

insert not_layoffs
select*
from layoffs;

select*,
row_number() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as  not_id
from not_layoffs;

with duplicate_cte as
(select*,
row_number() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as not_id
from not_layoffs)

select *
from duplicate_cte
where not_id > 1;


CREATE TABLE `otra_copia` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `not_id` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from otra_copia;

INSERT INTO otra_copia
select*,
row_number() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as not_id
from not_layoffs;

DELETE
FROM otra_copia
where not_id > 1;

select*
from otra_copia
where not_id > 1;


-- standardizing data

select company, TRIM( (company))
from otra_copia;

UPDATE otra_copia
SET company= TRIM(company);

select DISTINCT industry
from otra_copia
;

UPDATE otra_copia
SET industry='Crypto'
WHERE industry like 'Crypto%';

select DISTINCT country
from otra_copia
ORDER BY 1;

UPDATE otra_copia
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';

SELECT `date`
FROM otra_copia;

UPDATE otra_copia
SET `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE otra_copia
MODIFY COLUMN `date` DATE;


select*
FROM otra_copia
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

UPDATE otra_copia
SET industry= NULL
WHERE industry='';

SELECT *
from otra_copia
WHERE industry IS NULL
OR industry='';


SELECT *
FROM otra_copia
WHERE company ='Airbnb';

SELECT oc1.industry,oc2.industry
FROM otra_copia oc1
JOIN otra_copia oc2
    ON oc1.company=oc2.company
    AND oc1.location=oc2.location
WHERE (oc1.industry IS NULL OR oc1.industry='')
AND oc2.industry IS NOT NULL;

UPDATE otra_copia oc1
JOIN otra_copia oc2
      ON oc1.company=oc2.company
SET oc1.industry=oc2.industry
where (oc1.industry IS NULL OR oc1.industry='')
AND oc2.industry IS NOT NULL;

select*
FROM otra_copia;

DELETE
FROM otra_copia
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select*
FROM otra_copia
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM otra_copia;

ALTER TABLE otra_copia
DROP COLUMN not_id;


















