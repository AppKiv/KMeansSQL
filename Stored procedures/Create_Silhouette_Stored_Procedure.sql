USE [db_clustering]
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


