WITH rec_cte AS (
	SELECT idChild, hid, hidParent, 0 AS lvl
	FROM hm_NodeLinkPublishedHierarchy 
	WHERE idchild = 22667

	UNION ALL

	SELECT nlph.idChild, nlph.hid, nlph.hidParent, cte.lvl+1 AS lvl
	FROM rec_cte cte
	INNER JOIN hm_NodeLinkPublishedHierarchy nlph
	ON cte.hid = nlph.hidParent
)
SELECT * FROM rec_cte;
