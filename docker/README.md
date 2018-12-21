# docker version of SemEHR

Makes it dead easy to run SemEHR: just put a set of documents in a folder and then run SemEHR container over it.

## compile docker image locally
```
docker build -t cogstack/semehr - < Dockerfile
```

## run SemEHR docker image

### prerequisite
- select/create a host directory (let's call it `data` dir) for input fulltexts and outputs. There should be 3 subfolders:
    - input_docs: for putting full text documents; Add your own documents or two sample docs will be put in here for demostration purposes.
    - output_docs: for saving `temporary` NLP annotations;
    - smehr_results: for saving SeEHR results.
- (optional) create a SemEHR configuration file in `data` dir. If not, a default configuration will be used, i.e. `docker/docker_doc_based_settings.json`.
- (optional) [*Gazetteer settings*] A sample gazetteer will be used for NLP annotation. This is a list of entities used for a stroke subtyping study. It is recommended to use a UMLS gazetteer that bio-yodie can use. Due to license purpose, we cannot provide it. But very happy to support you to populate your own if you have got a UMLS license.

### run the container
```
docker run --name=semehr-test \
--mount type=bind,src=FULL PATH OF YOUR DATA FOLDER,dst=/data/ \
cogstack/semehr
```
If you have got a bio-yodie usable UMLS resource populated, you can use it like the following.
```
docker run --name=semehr-test \
--mount type=bind,src=FULL PATH OF YOUR DATA FOLDER,dst=/data/ \
--mount type=bind,src=FULL PATH OF YOUR UMLS RESOURCE FOR BIO-YODIE,dst=/opt/gcp/bio-yodie-1-2-1/bio-yodie-resources \
cogstack/semehr
```

## results
Each file in the `semehr_results` folder contains the annotations generated for a full text file. It contains three attributes at its 
top level as follows.
```JSON
{
  "sentences": [], 
  "annotations": [],
  "phenotypes": []
}
```
- `sentences` array contains the sentences start/end offsets.
- `annotations` array contains the UMLS concept mentions (details as follows).
- `phenotypes` array contains the mentions of entities in the customised gazetteer.


```JSON
{
      "ruled_by": [
        "negation_filters.json"
      ],
      "end": 1734,
      "pref": "Bleeding",
      "negation": "Negated",
      "sty": "Pathologic Function",
      "start": 1726,
      "study_concepts": [
        "Bleeding"
      ],
      "experiencer": "Patient",
      "str": "bleeding",
      "temporality": "Recent",
      "id": "cui-47",
      "cui": "C0019080"
    }
```
- `ruled_by` gives the rule set that the annotation matched. Generally, there are several types of rules of
negation, hypothetical, not a mention, other experiencer (see [here](https://github.com/CogStack/CogStack-SemEHR/tree/master/studies/rules) for full list). These rules were developed for clinical studies conducted on SLaM CRIS data. You can also create your own rules using the same syntax (regular expressions). Consider this as an extra improvement step on the embedded NLP model (i.e. bio-yodie for now) in SemEHR. 
- `study_concepts` the type of the annotation as specified in the study configuration (e.g., cancer can be mapped to many UMLS concepts).  So, essentially, a study concept denotes a list of UMLS CUIs, which is specified by the study designer. SemEHR repo's `studies` folder contains configurations of several studies conducted on SLaM CRIS.
- all other attribtues are general attributes of SemEHR as specified in the wiki.

*NB*: when a study configuration is provided, SemEHR will only apply rules on annotations that are relevant to the study and skip other ones. This means only those whose `study_concepts` is not empty will be checked by rules. 

## logging
A log file `semehr.log` will be generated in the above mentioned attached `data` folder. 
