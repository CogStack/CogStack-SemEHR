## Tutorial 1: SemEHR over Elasticsearch 
### Introduction
In this tutorial, we will illustrate how to deploy SemEHR results over an Elasticsearch
cluster and play with the results using SemEHR patient phenome viewer (browser based user interface). The tutorial uses a sample corpus - [20 discharge summaries](https://github.com/CogStack/CogStack-SemEHR/tree/master/tutorials/mtsamples-cohort/discharge_summaries) downloaded from http://www.medicaltranscriptionsamples.com/.

### Prerequisite
1. Create to a working folder for this tutorial: `/semehr_tutorial1` 
   (of couse, you can choose whatever folder path you like).
   ```bash
   mkdir /semehr_tutorial1
   cd /semehr_tutorial1
   ```
2. A UMLS gazetteer that bio-yodie (installed within SemEHR as the default NLP module) can use. Due to license purpose, we cannot provide it. If you have got a UMLS license, 
please follow instructions from [here](https://github.com/GateNLP/bio-yodie-resource-prep) to populate your own. 
Alternatively, we are happy to provide a copy if you can let us know you have got your UMLS license. (contact: honghan.wu@gmail.com)
   ```bash
   cp YOUR_UMLS_GAZETTEER_FOLDER /semehr_tutorial1/bio-yodie-resources -R
   ```
3. Install docker: follow instructions on official docker website: [install docker](https://docs.docker.com/install/).
4. Install docker-compose: follow [this page](https://docs.docker.com/compose/install/).

5. clone SemEHR repo
   ```bash
   git clone https://github.com/CogStack/CogStack-SemEHR.git
   ```
   
### 1. run server containers
1. go to docker compose files folder
    ```bash
    cd /semehr_tutorial1/CogStack-SemEHR/tutorials/tutorial1_compose_files
    ```
2. run the servers
Note: you can edit `semehr-tutorial1-servers-compose.yml` to either fit your environment if you use different folder path or other settings; or tweak the server parameters.
    ```bash
    docker-compose -f semehr-tutorial1-servers-compose.yml up -d
    ```
    This will start a two-node elasticsearch cluster and a web server for hosting

### 2. run SemEHR
Note: you can edit `semehr-tutorial-run-compose.yml` to fit your environment.
```bash
docker-compose -f semehr-tutorial-run-compose.yml run semehr
```

### 3. play with the results
You can play with the results using either of the options below.

- use SemEHR patient phenome viewer
Put the url `http://IP_ADDR:8080/SemEHR.html` into your browser (Chrome or Firefox). 
(Note: there are 20 patients, IDs from P001 to P020, each of whom has a discharge summary in the corpus) 
    - search a patient: put `P001` as an example into the search box and click `search`
    - (optionally) choose a disease model from the drop list to show how the UI works when
    a predefined disease model (essentially a list of phenotypes a study is looking for) is used.  
    - Screenshot 1: phenotype list page
    ![screenshot1](https://raw.githubusercontent.com/CogStack/CogStack-SemEHR/master/tutorials/imgs/Screenshot_tutorial1.1.png "Patient Phenotype Viewer")
    This page lists the phenotypes (mapped to [Human Phenotype Ontology](https://hpo.jax.org/)) for a given patient. If a diseae model is selected, the phenotypes are put into two sections: top section lists all phenotypes defined in the selected disease model; bottom section lists the rest. There are three disease models defined. `Example - Conditions for John's Study` is an example model (of 6 common conditions). The other two models are extracted from rare disease models of Genomics England's 100K project.
    - Screenshot 2: detailed phenotype mention page
     ![screenshot2](https://raw.githubusercontent.com/CogStack/CogStack-SemEHR/master/tutorials/imgs/Screenshot_tutorial1.2.png "Patient Phenotype Viewer shows contextualised phenotype mentinos")
     When you click a phenotype from the list on the previous page, this page will pop out. It shows two things: 1) a summary about the phenotype in the patient's EHRs (how many mentions and how they are classified into different contextualised mentions); 2) when you click on any of the numbers, the page will show the original EHR document(s) with respective mentions highlighted.
     - Screenshot 3: patient's document list
     ![screenshot3](https://raw.githubusercontent.com/CogStack/CogStack-SemEHR/master/tutorials/imgs/Screenshot_tutorial.1.3.png "Patient Phenotype Viewer shows all the documents of the searched patient")
     

- check elasticsearch indices (replace `IP_ADDR` with your machine's IP address)
    - list all SemEHR indices
    ```bash
    curl -X GET "http://IP_ADDR:8200/_cat/indices/"
    ```
    it will show something like the following
    ```
    green open semehr_ctx_concepts HZ9NwkhBTAu4uSK586XuJg 1 1 947 0 423.4kb 191.6kb
    green open eprdoc              vJuhrEomQeimlv_9kSFsHg 1 1  20 0 145.5kb  72.7kb
    green open semehr_docs         U3tV7qp8RFeFyZsYySW3aQ 1 1  20 0 814.3kb 406.4kb
    green open semehr_patients     _EwwE2szQ1epHrMXymFiBA 1 1  20 0   1.2mb 681.4kb
    ```
    - query a patient 
    ```bash
    curl -X GET "http://IP_ADDR:8200/semehr_patients/_search/?q=P001&size=1&pretty"
    ```
    it will give you something like
    ```json
    {
      "took": 2,
      "timed_out": false,
      "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
      },
      "hits": {
        "total": {
          "value": 1,
          "relation": "eq"
        },
        "max_score": 2.6390574,
        "hits": Array[1][
          {
            "_index": "semehr_patients",
            "_type": "patient",
            "_id": "P001",
            "_score": 2.6390574,
            "_source": {
              "articles": Array[1][
                {
                  "fulltext": "Description: Incision and drainage, first metatarsal head, left foot with culture and sensitivity.\n(Medical Transcription Sample Report)\nADMITTING DIAGNOSIS: Abscess with cellulitis, left foot.\n\nDISCHARGE DIAGNOSIS: Status post I&D, left foot.\n\nPROCEDURES: Incision and drainage, first metatarsal head, left foot with culture and sensitivity.\n\nHISTORY OF PRESENT ILLNESS: The patient presented to Dr. X's office on 06/14/07 complaining of a painful left foot. The patient had been treated conservatively in office for approximately 5 days, but symptoms progressed with the need of incision and drainage being decided.\n\nMEDICATIONS: Ancef IV.\n\nALLERGIES: ACCUTANE.\n\nSOCIAL HISTORY: Denies smoking or drinking.\n\nPHYSICAL EXAMINATION: Palpable pedal pulses noted bilaterally. Capillary refill time less than 3 seconds, digits 1 through 5 bilateral. Skin supple and intact with positive hair growth. Epicritic sensation intact bilateral. Muscle strength +5/5, dorsiflexors, plantar flexors, invertors, evertors. Left foot with erythema, edema, positive tenderness noted, left forefoot area.\n\nLABORATORY: White blood cell count never was abnormal. The remaining within normal limits. X-ray is negative for osteomyelitis. On 06/14/07, the patient was taken to the OR for incision and drainage of left foot abscess. The patient tolerated the procedure well and was admitted and placed on vancomycin 1 g q.12h after surgery and later changed Ancef 2 g IV every 8 hours. Postop wound care consists of Aquacel Ag and dry dressing to the surgical site everyday and the patient remains nonweightbearing on the left foot. The patient progressively improved with IV antibiotics and local wound care and was discharged from the hospital on 06/19/07 in excellent condition.\n\nDISCHARGE MEDICATIONS: Lorcet 10/650 mg, dispense 24 tablets, one tablet to be taken by mouth q.6h as needed for pain. The patient was continued on Ancef 2 g IV via PICC line and home health administration of IV antibiotics.\n\nDISCHARGE INSTRUCTIONS: Included keeping the foot elevated with long periods of rest. The patient is to wear surgical shoe at all times for ambulation and to avoid excessive ambulation. The patient to keep dressing dry and intact, left foot. The patient to contact Dr. X for all followup care, if any problems arise. The patient was given written and oral instruction about wound care before discharge. Prior to discharge, the patient was noted to be afebrile. All vitals were stable. The patient's questions were answered and the patient was discharged in apparent satisfactory condition. Followup care was given via Dr. X' office. ",
                  "erpid": "discharge_summary_01.txt"
                }
              ],
              "anns": Array[137][
                {
                  "ruled_by": Array[0][
                    
                  ],
                  "end": 325,
                  "contexted_concept": "E30ABFEBB4AA4E0638D61E46A6E8AF9F",
                  "pref": "Culture",
                  "negation": "Affirmed",
                  "id": "cui-1",
                  "start": 318,
                  "eprid": "discharge_summary_01.txt",
                  "study_concepts": Array[0][
                    
                  ],
                  "experiencer": "Patient",
                  "str": "culture",
                  "temporality": "Recent",
                  "sty": "Laboratory Procedure",
                  "cui": "C2242979"
                },
             ...
    ```
