/* -----------------------------------------------------------
   
intit_name_clean.cypher
Initialise full-text search for Organisation names
   1. Normalises every Organisation name to ascii-lower-alnum

   Run once after the sponsor-register ETL, or after you wipe the DB.
----------------------------------------------------------- */

CALL apoc.periodic.commit(
  "
  MATCH (o:Organisation)
  WHERE o.name_clean IS NULL        // skip nodes already processed
  WITH o LIMIT $batch
  SET  o.name_clean = apoc.text.clean(o.name)
  RETURN count(*)                   // required by apoc.periodic.commit
  ",
  {batch:10000}
);
