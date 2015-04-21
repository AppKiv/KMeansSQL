USE db_clustering
GO

--Perform k-means clustering
EXEC usp_cluster
	@tbl_name = 'tbl_clusters',
	@pk_col = 'id',
	@x_val_col = 'x_val',
	@y_val_col = 'y_val',
	@k = 3,
	@new_tbl_name = 'tbl_clusters_k3'

--Evaluate silhouette
EXEC usp_silhouette
	@table_name = 'tbl_clusters_k3',
	@pk_col = 'pk_col',
	@x_val_col = 'x_val',
	@y_val_col = 'y_val',
	@clust_col = 'clust_id'


--View clustered data
SELECT *
FROM tbl_clusters_k3