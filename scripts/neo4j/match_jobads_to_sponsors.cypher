/* 
match_job_ads_to_sponsors.cypher
-----------------------------------------------------------------
   Enrich :JobAd nodes with sponsor information
   • Expects    $jobBatch = [ {id:'123', company:'IBM UK LTD', …}, … ]
   • Adds / updates:
       j.sponsor_possible = true
       j.routes           = [...]
       j.match_score, j.last_matched_ts
     and a :POSTED_BY relationship with r.match_score
------------------------------------------------------------------ */

CALL apoc.periodic.iterate(
"
  UNWIND $jobBatch AS job
  RETURN job
",
"
  /* 1 ▸ normalise advert company once */
  WITH job, apoc.text.clean(job.company) AS comp_clean

  /* 2 ▸ find best matching sponsor (inline logic) */
  CALL {
    WITH comp_clean
    CALL db.index.fulltext.queryNodes('orgNameFT', comp_clean)
    YIELD node AS org, score
    ORDER BY score DESC LIMIT 1
    WITH org,
         coalesce(score,0.0) AS ft,
         apoc.text.jaroWinklerDistance(comp_clean, org.name_clean) AS jw
    WHERE ft >= 0.60 OR jw >= 0.85
    RETURN org,
           apoc.number.max([ft,jw]) AS matchScore
  }

  /* 3 ▸ skip batch row if nothing qualified */
  WITH job, comp_clean, org, matchScore
  WHERE org IS NOT NULL

  /* 4 ▸ merge / update JobAd */
  MERGE (j:JobAd {id: job.id})
  SET   j += job,
        j.company_clean    = comp_clean,
        j.sponsor_possible = true,
        j.match_score      = matchScore,
        j.last_matched_ts  = datetime()

  /* 5 ▸ relationship + score */
  MERGE (j)-[r:POSTED_BY]->(org)
  SET   r.match_score = matchScore

  /* 6 ▸ visa routes */
  WITH j, org
  MATCH (org)-[:OFFERS_ROUTE]->(rt:Route)
  WITH j, collect(DISTINCT rt.name) AS routes
  SET  j.routes = routes
",
{batchSize:1000, parallel:false, params:{jobBatch:$jobBatch}}
)
YIELD batches, total, errorMessages
RETURN batches,total,errorMessages;
