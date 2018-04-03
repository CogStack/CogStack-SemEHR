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
* sample ICD10 to UMLS mapping with label SPARQL query
```SPARQL
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT distinct ?umls, ?label
FROM <http://bioportal.bioontology.org/ontologies/ICD10>
WHERE {
  <http://purl.bioontology.org/ontology/ICD10/A01> <http://bioportal.bioontology.org/ontologies/umls/cui> ?umls;
                                                   <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
  ?s <http://bioportal.bioontology.org/ontologies/umls/isRoot> true.
}
```
