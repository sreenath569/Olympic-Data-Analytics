SELECT *
FROM Athletes

--total Athletes count by country
SELECT Country,COUNT(*) Athletes_count
FROM Athletes
GROUP BY Country
ORDER BY COUNT(*) DESC;

--total Athletes count by Discipline
SELECT Discipline,COUNT(*) Athletes_count
FROM Athletes
GROUP BY Discipline
ORDER BY COUNT(*) DESC;

--total Athletes count by Country,Discipline
SELECT Country,Discipline,COUNT(*) Athletes_count
FROM Athletes
GROUP BY Country,Discipline
ORDER BY Country,COUNT(*) DESC;