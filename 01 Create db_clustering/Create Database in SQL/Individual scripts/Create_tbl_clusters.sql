--Generate cluster data
USE db_clustering
GO

--Table to hold dummy data
CREATE TABLE tbl_clusters (
	ID INT IDENTITY(1,1) PRIMARY KEY,
	x_val FLOAT,
	y_val FLOAT
);

--Loop counter
DECLARE @cnt INT;

--Cluster 1
SET @cnt = 0
WHILE(@cnt < 75)
BEGIN
	INSERT INTO tbl_clusters(x_val,y_val)
	SELECT (RAND()*30),(RAND()*30)
	SET @cnt = @cnt + 1
END

--Cluster 2
SET @cnt = 0;
WHILE(@cnt < 75)
BEGIN
	INSERT INTO tbl_clusters(x_val,y_val)
	SELECT (RAND()*20) + 80,(RAND()*30)
	SET @cnt = @cnt + 1
END

--Cluster 3
SET @cnt = 0;
WHILE(@cnt < 20)
BEGIN
	INSERT INTO tbl_clusters(x_val,y_val)
	SELECT (RAND()*20)+ 40,(RAND()*20) + 75
	SET @cnt = @cnt + 1
END

--Outliers
SET @cnt = 0;
WHILE(@cnt < 10)
BEGIN
	INSERT INTO tbl_clusters(x_val,y_val)
	SELECT (RAND()*100),(RAND()*100)
	SET @cnt = @cnt + 1
END