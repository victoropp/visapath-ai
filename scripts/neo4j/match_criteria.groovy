
/*
match_criteria.groove

  INPUT:
    $lucene – a Lucene query string, e.g. 'ibm~'
    $clean  – the cleaned-up company name, e.g. 'ibm'

  OUTPUT:
    org         – the Organisation node
    matchScore  – the best match score (float)
*/

// Fulltext lookup on Organisation.name
CALL db.index.fulltext.queryNodes('orgNameFT', $lucene)
YIELD node AS org, score AS ft

// Jaro–Winkler similarity on the cleaned name
WITH org, ft,
     apoc.text.jaroWinklerDistance($clean, org.name_clean) AS jw

// Only keep strong matches
WHERE ft >= 0.60 OR jw >= 0.85

// Return the organisation and the maximum of the two scores
RETURN org, apoc.number.max([ft, jw]) AS matchScore;
