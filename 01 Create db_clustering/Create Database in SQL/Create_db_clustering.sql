USE MASTER 
GO

CREATE DATABASE db_clustering
GO

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
GO

CREATE PROCEDURE [dbo].[usp_cluster]
	--target table name
	@tbl_name VARCHAR(MAX),
	--x value column name
	@x_val_col VARCHAR(1000),
	--y value column name
	@y_val_col VARCHAR(1000),
	--primary key column(unique identifier)
	@pk_col VARCHAR(1000),
	--table name to hold clustered results
	@new_tbl_name VARCHAR(1000),
	--number of clusters
	@k INT
AS

--Check if @new_tbl_name already table exists
	--if so, break
IF OBJECT_ID(@new_tbl_name) IS NOT NULL
BEGIN
	PRINT (@new_tbl_name + ' already exists')
	RETURN
END
--If tables exist, drop them
IF OBJECT_ID('tempdb..##tbl_cluster') IS NOT NULL
	DROP TABLE ##tbl_cluster;
IF OBJECT_ID('tempdb..#tbl_clust_centers') IS NOT NULL
	DROP TABLE #tbl_clust_centers;
IF OBJECT_ID('tempdb..#tbl_record') IS NOT NULL
	DROP TABLE #tbl_record;

--Create table to hold cluster centers
CREATE TABLE #tbl_clust_centers
             (
                ID INT IDENTITY(1,1),
                clust_x_val FLOAT   ,
                clust_y_val FLOAT
             );
			 
-- Populate cluster center table with placeholders(zeros)
INSERT INTO #tbl_clust_centers
            (
				clust_x_val,
				clust_y_val
            )
SELECT     TOP (@k) 0,
           0
FROM       sys.all_columns a
           CROSS JOIN sys.all_columns

--Create table to hold previous values of cluster center to track changes in centers
CREATE TABLE #tbl_record
             (
                          ID INT           ,
                          clust_x_val FLOAT,
                          clust_y_val FLOAT
             );

--Declare variables
DECLARE 
	--Loop counter
	@cnt INT, 
	--Holds dynamic sql statement
	@sql VARCHAR(MAX);

--Create global temp table ##tbl_cluster to hold data with randomly assigned clusters
SELECT
	@sql = 'SELECT ' + @pk_col + ' pk_col,' + @x_val_col + ' x_val,' + @y_val_col + ' y_val,CAST(RAND(CHECKSUM(NEWID())) *' + CAST(@k AS varchar) + ' as INT) + 1 clust_id INTO ##tbl_cluster FROM ' + @tbl_name + ' WHERE ' + @x_val_col + ' IS NOT NULL AND ' + @y_val_col + ' IS NOT NULL'
EXEC (@sql)

--Find cluster centers now that the clusters have been randomly assigned to the data values
UPDATE #tbl_clust_centers
SET    clust_x_val =
       (
	   SELECT AVG(x_val + 0.0)
       FROM    ##tbl_cluster A
       WHERE   A.clust_id = #tbl_clust_centers.ID
       )
       ,
       clust_y_val =
       (
	   SELECT AVG(y_val + 0.0)
       FROM    ##tbl_cluster A
       WHERE   A.clust_id = #tbl_clust_centers.ID
       )

--Set @cnt to track the number loops
SET @cnt = 0;
--Keep Looping until cluster centers stop moving
WHILE (1=1)
	BEGIN
	--Every 10 loops, record the current cluster center into #tbl_record
	IF ((@cnt % 10) = 0)
	BEGIN
		--If a null occurs, break
		IF ((SELECT COUNT(*) 
			FROM #tbl_clust_centers 
			WHERE 
			clust_x_val IS NULL
			OR clust_y_val IS NULL) > 0)
		BEGIN
			BREAK;
		END

		INSERT #tbl_record
			SELECT *
			FROM #tbl_clust_centers
	END;

	--Assign data values to nearest cluster
	UPDATE ##tbl_cluster
	SET ##tbl_cluster.clust_id = D.nw_clust_id
	FROM 
	(
		SELECT *
			FROM 
			(
				SELECT
				A.pk_col,
				A.clust_id,
				B.ID nw_clust_id,
				ROW_NUMBER() OVER (PARTITION BY A.pk_col ORDER BY SQRT(SQUARE(B.clust_x_val - A.x_val) + SQUARE((clust_y_val - A.y_val)))) rn,
				SQRT(SQUARE(B.clust_x_val - A.x_val) + SQUARE((clust_y_val - A.y_val))) dist
				FROM ##tbl_cluster A
				CROSS JOIN #tbl_clust_centers B
			) C
		WHERE rn = 1
	) D
	WHERE ##tbl_cluster.pk_col = D.pk_col

	--Find new cluster centers
	UPDATE #tbl_clust_centers
	SET	clust_x_val = 
		(
			SELECT AVG(x_val + 0.0)
			FROM ##tbl_cluster A
			WHERE clust_id = #tbl_clust_centers.ID
		),
		clust_y_val = 
		(
			SELECT AVG(y_val + 0.0)
			FROM ##tbl_cluster A
			WHERE clust_id = #tbl_clust_centers.ID
		)

	--Every 10 loops, use #tbl_record to check for change in cluster centers
	--If no change, then break
	--Else continue
	IF ((@cnt % 10) = 0) 
	BEGIN
		IF 
		(
		0 =	(
				SELECT COUNT(*) 
				FROM
				(	
					SELECT A.ID, A.clust_x_val, A.clust_y_val
					FROM 
					(
						SELECT * FROM #tbl_record 
						UNION ALL 
						SELECT * FROM #tbl_clust_centers
					) A
					GROUP BY	A.ID,
								A.clust_x_val,
								A.clust_y_val 
					HAVING COUNT(*) != 2
				) B
			)
		)
		BEGIN
			BREAK;
		END
		--cleanup tbl check
		DELETE FROM #tbl_record
	END;
	--Increment loop counter
	SET @cnt = @cnt + 1;
END

--Store clustered data into table with the name from @new_tbl_name
EXEC('SELECT * INTO ' + @new_tbl_name + ' FROM ##tbl_cluster')

--Cleanup tables if they exist
IF OBJECT_ID('tempdb..##tbl_cluster') IS NOT NULL
	DROP TABLE ##tbl_cluster;
IF OBJECT_ID('tempdb..#tbl_clust_centers') IS NOT NULL
	DROP TABLE #tbl_clust_centers;
IF OBJECT_ID('tempdb..#tbl_record') IS NOT NULL
	DROP TABLE #tbl_record;
GO

CREATE PROCEDURE [dbo].[usp_silhouette]
	@table_name VARCHAR(100),
	@pk_col VARCHAR(100),
	@clust_col VARCHAR(100),
	@x_val_col VARCHAR(100),
	@y_val_col VARCHAR(100)
	
AS

DECLARE @sql VARCHAR(MAX)
SET @sql =
'SELECT ( SELECT COUNT(DISTINCT clust_id) FROM ' + @table_name + ') k,
AVG( CASE WHEN G.avg_dist_diff_clust > C.avg_dist_same_clust THEN (G.avg_dist_diff_clust - C.avg_dist_same_clust) / G.avg_dist_diff_clust ELSE (G.avg_dist_diff_clust - C.avg_dist_same_clust) / C.avg_dist_same_clust END) silhoutte FROM (SELECT A.' +
@pk_col + ',AVG(SQRT(SQUARE(A.' + @x_val_col + '- B.' + @x_val_col + ') + SQUARE(A.' + @y_val_col + '- B.' + @y_val_col + '))) avg_dist_same_clust FROM ' + @table_name +
' A INNER JOIN ' + @table_name +
' B ON A.' + @clust_col + ' = B.' + @clust_col + ' GROUP BY A.' + @pk_col +
') C INNER JOIN (SELECT  *,ROW_NUMBER() OVER (PARTITION BY F.' + @pk_col +
' ORDER BY avg_dist_diff_clust) rn FROM (SELECT D.' + @pk_col +
',E.' + @clust_col + ',AVG(SQRT(SQUARE(D.' + @x_val_col + ' - E.' + @x_val_col +
') + SQUARE(D.' + @y_val_col + ' - E.' + @y_val_col +
'))) avg_dist_diff_clust FROM ' + @table_name +
' D INNER JOIN ' + @table_name +
' E ON D.' + @clust_col + ' != E.' + @clust_col +
' GROUP BY D.' + @pk_col +
', E.' + @clust_col +
') F ) G ON C.' + @pk_col + '= G.' + @pk_col +
' WHERE G.rn = 1'

EXEC (@sql)

GO
