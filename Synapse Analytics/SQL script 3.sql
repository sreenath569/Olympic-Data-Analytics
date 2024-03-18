--SELECT *
--FROM EntriesGender
--order by Discipline
--
--total athletes pct by Gender
--SELECT 100*SUM(CAST(Female AS DECIMAL))/(SUM(Female)+SUM(Male)) Female_pct, 100*SUM(CAST(Male AS DECIMAL))/(SUM(Female)+SUM(Male)) Male_pct
--FROM EntriesGender

WITH total_athletes AS(
select SUM(Total) total_count
FROM EntriesGender
)

--athletes total pct by Discipline
SELECT Discipline, 100*CAST(Total AS DECIMAL)/(SELECT total_count from total_athletes)
FROM EntriesGender
ORDER BY 2 DESC
