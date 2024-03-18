SELECT *
FROM Medals

--medals won by country
SELECT Team_Country,Gold,Silver,Bronze
FROM Medals
order by Gold DESC,Silver DESC,Bronze DESC
