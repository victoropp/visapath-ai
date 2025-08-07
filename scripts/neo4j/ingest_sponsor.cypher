/* -----------------------------------------------------------
  injest_sponsor.cypher
----------------------------------------------------------- */



CREATE CONSTRAINT ON (o:Organisation) ASSERT o.name IS UNIQUE;
CREATE CONSTRAINT ON (loc:Location) ASSERT (loc.county, loc.city) IS UNIQUE;
CREATE CONSTRAINT ON (r:Route) ASSERT r.name IS UNIQUE;
