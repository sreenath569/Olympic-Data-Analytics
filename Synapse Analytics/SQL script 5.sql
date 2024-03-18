SELECT *
FROM Teams

--testing
SELECT count(distinct Event)
FROM Teams

--distinct events by Discipline
SELECT distinct Discipline, Event
FROM Teams
ORDER BY Discipline,Event