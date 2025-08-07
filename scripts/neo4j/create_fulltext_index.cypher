CREATE FULLTEXT INDEX orgNameFT IF NOT EXISTS
FOR (o:Organisation) ON EACH [o.name_clean];
