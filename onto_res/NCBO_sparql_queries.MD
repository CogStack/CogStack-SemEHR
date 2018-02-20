[NCBO SPARQL EndPoint](http://sparql.bioontology.org/)
* get root level ICD10 concepts mappings to UMLS CUIs
```SPARQL
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT distinct ?s ?umls
FROM <http://bioportal.bioontology.org/ontologies/ICD10>
WHERE {
  ?s <http://bioportal.bioontology.org/ontologies/umls/cui> ?umls.
  ?s <http://bioportal.bioontology.org/ontologies/umls/isRoot> true.
}
```
