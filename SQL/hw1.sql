DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE weight > 300;
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear 
  FROM master
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*) 
  FROM master
  GROUP BY birthyear
  ORDER BY birthyear ASC
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  WITH temp (birthyear, avgheight, count) AS (
    SELECT birthyear, AVG(height) , COUNT(*) 
    FROM master
    GROUP BY birthyear
  )
  SELECT * 
  FROM temp 
  WHERE temp.avgheight > 70
  ORDER BY birthyear ASC
;


-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, h.playerid, yearid
  FROM halloffame as h, master as m
  WHERE m.playerid = h.playerid AND inducted = 'Y'
  ORDER BY yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT m.namefirst, m.namelast, m.playerid, c.schoolid, h.yearid
  FROM master AS m, collegeplaying AS c, halloffame AS h, schools AS s
  WHERE c.playerid = h.playerid AND h.playerid = m.playerid 
  		AND c.schoolid = s.schoolid AND s.schoolstate = 'CA' 
  		AND inducted = 'Y' 
  ORDER BY h.yearid DESC, c.schoolid, c.playerid ASC
;

-- Question 2iiis		
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  WITH names(playerid, namefirst, namelast) AS (
  	SELECT h.playerid, namefirst, namelast 
  	FROM halloffame AS h, master AS m 
  	WHERE h.playerid = m.playerid AND inducted = 'Y')

  SELECT n.playerid, namefirst, namelast, schoolid
  FROM names AS n 
  LEFT OUTER JOIN collegeplaying AS c 
  ON c.playerid = n.playerid
  ORDER BY playerid DESC, schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT m.playerid, namefirst, namelast, b.yearid, 
  		CAST ((1.0 * (h - h2b - h3b - hr) + 2.0 * h2b + 3.0 * h3b + 4.0 * hr) / ab AS float(25))  AS slg
  FROM master AS m, batting AS b
  WHERE ab > 50 AND m.playerid = b.playerid 
  ORDER BY slg DESC, b.yearid, m.playerid ASC
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
   WITH allScores(playerid, sumh1, sumh2, sumh3, sumhr, sumab) AS (
   		SELECT playerid, SUM(h - h2b - h3b - hr), SUM(h2b), SUM(h3b), SUM(hr), SUM(ab) 
   		FROM batting
   		GROUP BY playerid
   		)

   SELECT DISTINCT m.playerid, m.namefirst, m.namelast, 
   		  CAST ((1.0*a.sumh1 + 2.0*a.sumh2 + 3.0*a.sumh3 + 4.0*a.sumhr) / a.sumab AS float(25)) AS lslg
   FROM master AS m, batting AS b, allScores AS a
   WHERE sumab != 0 AND sumab > 50 AND m.playerid = b.playerid AND a.playerid = m.playerid
   ORDER BY lslg DESC, m.playerid ASC
   LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
	WITH allScores(playerid, sumh1, sumh2, sumh3, sumhr, sumab) AS (
   		SELECT playerid, SUM(h - h2b - h3b - hr), SUM(h2b), SUM(h3b), SUM(hr), SUM(ab) 
   		FROM batting
   		GROUP BY playerid
   		),
		anotherPlayer(playerid, lslga) AS(
		SELECT playerid, CAST ((1.0*sumh1 + 2.0*sumh2 + 3.0*sumh3 + 4.0*sumhr) / sumab AS float(25)) 
		FROM allScores
		WHERE playerid = 'mayswi01'
		),
		completeList(playerid, namefirst, namelast, lslgc) AS (
   		SELECT DISTINCT m.playerid, m.namefirst, m.namelast, 
   		  CAST ((1.0*a.sumh1 + 2.0*a.sumh2 + 3.0*a.sumh3 + 4.0*a.sumhr) / a.sumab AS float(25)) AS lslgc
   		FROM master AS m, batting AS b, allScores AS a, anotherPlayer AS p
   		WHERE sumab != 0 AND sumab > 50 AND m.playerid = b.playerid AND a.playerid = m.playerid 
   		) 
   
   SELECT c.namefirst, c.namelast, c.lslgc
   FROM completeList AS c, anotherPlayer AS a
   WHERE c.lslgc > a.lslga
   ORDER BY c.lslgc DESC
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg, STDDEV(salary)
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid ASC
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH 
    maxAndmin (maxv, minv) AS (
	SELECT MAX(salary), MIN(salary)
	FROM salaries
	WHERE yearid = 2016
	),
    bins (binid, playerid, salary) AS (
  	SELECT LEAST(WIDTH_BUCKET(salary, m.minv, m.maxv, 10), 10), playerid, salary
  	FROM salaries, maxAndmin AS m
  	WHERE yearid = 2016
  	GROUP BY minv, maxv, salary, playerid
  	)
   SELECT binid -1 AS binid, m.minv + (binid - 1) * (m.maxv - m.minv) / 10 AS low, m.minv + binid * (m.maxv - m.minv) / 10 AS high, COUNT(*)
   FROM bins, maxAndmin AS m
   GROUP BY binid, maxv, minv
   ORDER BY binid ASC
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH overall (yearid, min, max, avg) AS (
  	SELECT yearid, MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg
  	FROM salaries
  	GROUP BY yearid
  	ORDER BY yearid ASC
  	)
  SELECT a.yearid, a.min - b.min AS mindiff, a.max - b.max AS maxdiff, a.avg - b.avg AS avgdiff
  FROM overall AS a, overall AS b
  WHERE a.yearid > 1985 AND b.yearid = a.yearid - 1
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT m.playerid, namefirst, namelast, salary, s.yearid
  FROM master AS m, salaries AS s
  WHERE m.playerid = s.playerid AND s.yearid = 2000 
  		AND s.salary >= ALL 
  			(SELECT salary FROM salaries  WHERE yearid = 2000)
  UNION 

  SELECT m.playerid, namefirst, namelast, salary, s.yearid
  FROM master AS m, salaries AS s
  WHERE m.playerid = s.playerid AND s.yearid = 2001 
  		AND s.salary >= ALL 
  			(SELECT salary FROM salaries  WHERE yearid = 2001)

  ORDER BY yearid ASC 
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid, MAX(salary) - MIN(salary) AS diffAvg
  FROM allstarfull AS a, salaries AS s 
  WHERE a.playerid = s.playerid AND a.yearid = 2016 AND s.yearid = 2016
  GROUP BY a.teamid
  ORDER BY a.teamid
;