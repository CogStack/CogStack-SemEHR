# SemEHR: Surfacing Semantic Data from Clinical Notes in Electronic Health Records for Tailored Care, Trial Recruitment and Clinical Research

Built upon off-the-shelf toolkits including a Natural Language Processing (NLP) pipeline ([Bio-Yodie](https://gate.ac.uk/applications/bio-yodie.html])) and an enterprise search system ([CogStack](https://github.com/CogStack/CogStack)), SemEHR implements a generic information extraction (IE) and retrieval infrastructure by identifying contextualised mentions of a wide range of biomedical concepts from unstructured clinical notes. Its IE functionality features an adaptive and iterative NLP mechanism where specific requirements and fine-tuning can be fulfilled and realised on a study basis. NLP annotations are further assembled at patient level and extended with clinical and EHR-specific knowledge to populate a panorama for each patient, which comprises a) longitudinal semantic data views and b) structured medical profile(s). The semantic data is serviced via ontology-based search and analytics interfaces to facilitate clinical studies.  

![System Achitecture](https://raw.githubusercontent.com/CogStack/SemEHR/master/resources/SystemArch.png "System Achitecture")

With SemEHR, the clinical variables hidden in clinical notes are surfaced via a set of search interfaces. A typical process to answer a clinical question (e.g. patients with hepatitis c), which previously might involve NLP, turns into one or a few google-style searches, for which SemEHR will pull out the cohort of relevant patients, populate patient-level summaries - numbers of contextualised concept mentions (e.g. 2nd patient has 16 total mentions of the disease, 15 of them were positive and 1 was historical), and link each mention to its original clinical note.

## updates
- (14 Sept 2017) SemEHR has been accepted to present at [UK Publich Health Science Conference](http://www.ukpublichealthscience.org/) and to be published in [The Lancet](http://www.thelancet.com/).
- (15 March 2017) An SemEHR instance has been deployed on MIMICIII on KCL's Rosalind to facilitate researches on the public EHR data.

