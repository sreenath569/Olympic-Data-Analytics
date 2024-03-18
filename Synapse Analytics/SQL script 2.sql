SELECT *
FROM Coaches
order by Country

--testing
SELECT count(distinct Event)
FROM Coaches

--total coaches by country
SELECT Country,COUNT(*) Coaches_count
FROM Coaches 
GROUP BY Country
ORDER BY COUNT(*) DESC


--total coaches by Discipline
SELECT Discipline,COUNT(*) Coaches_count
FROM Coaches 
GROUP BY Discipline
ORDER BY COUNT(*) DESC


--total coaches by Country & Discipline
SELECT Country,Discipline,COUNT(*) Coaches_count
FROM Coaches 
GROUP BY Country, Discipline
ORDER BY Country,COUNT(*) DESC