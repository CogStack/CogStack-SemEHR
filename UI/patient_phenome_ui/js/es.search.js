/**
 * Created by honghan.wu on 21/04/2017.
 */
if (typeof semehr == "undefined"){
    var semehr = {};
}

(function($) {
    if(typeof semehr.search == "undefined") {

        semehr.search = {
            __indice_index: 0,
            __es_need_login: false,
            _es_client: null,
            __es_server_url: null,
            __es_port: 8200,
            __es_index: "semehr_patients", //epr_documents_bioyodie
            __es_type: "patient", //patient type
            __es_concept_type: "ctx_concept",
            __es_concept_index: "semehr_ctx_concepts",
            __es_fulltext_index: ["eprdoc"],
            __es_fulltext_type: "docs",
            _full_text_attr: ['fulltext'],
            _user_id: "not_logged",
            _fdid: '_id',
            __discharge_summary_type: "Discharge summary",
            _es_doc_patient_id_field: ["patient_id"],
            _es_doc_type_field: ["document_description"],
            _es_doc_date_field: ["document_dateadded"],
            
            // __es_need_login: false,
            // _es_client: null,
            // __es_server_url: "http://10.200.102.23:9200/",
            // __es_index: "mimic", //epr_documents_bioyodie
            // __es_type: "patient", //patient type
            // __es_concept_type: "ctx_concept",
            // __es_fulltext_index: ["mimic"],
            // __es_fulltext_type: "eprdoc",
            // _full_text_attr: ['fulltext'],
            // _user_id: "not_logged",
            // _fdid: '_id',
            // __discharge_summary_type: "Discharge summary",
            // _es_doc_patient_id_field: "patientId",
            // _es_doc_type_field: "docType",
            // _es_doc_date_field: "gooddate",

            initESClient: function(){
                if (semehr.search.__es_server_url == null) {
                    semehr.search.setupES();
                }
                else{
                    semehr.search.doInit();
                }
            },

            doInit: function(){
                if (semehr.search.__es_need_login){
                    semehr.search.easyLogin();
                }else{
                    semehr.search._es_client = new $.es.Client({
                        hosts: semehr.search.__es_server_url
                    });
                    semehr.search._es_client.ping({
                        requestTimeout: 30000,
                    }, function (error) {
                        if (error) {
                            console.error('elasticsearch cluster is down!');
                            swal('seems elasticsearch cluster is down!');
                        } else {
                            console.log('All is well');
                        }
                    });
                }
            },

            easyLogin: function(){
                swal.setDefaults({
                    confirmButtonText: 'Next &rarr;',
                    showCancelButton: true,
                    animation: false,
                    progressSteps: ['1', '2']
                })

                var steps = [
                    {
                        title: 'Login',
                        text: 'name',
                        input: 'text',
                    },
                    {
                        title: 'Login',
                        text: 'password',
                        input: 'password',
                        confirmButtonText: 'login'
                    }
                ]

                swal.queue(steps).then(function (result) {
                    swal.resetDefaults();
                    swal('login...');
                    swal.showLoading();
                    semehr.search._es_client = $.es.Client({
                        host: [
                            {
                                host: semehr.search.__es_server_url,
                                auth: result[0] + ':' + result[1],
                                protocol: 'https',
                                port: semehr.search.__es_port
                            }
                        ]
                    });
                    semehr.search._es_client.ping({
                        requestTimeout: 30000,
                    }, function (error) {
                        if (error) {
                            swal({
                                title: 'something is wrong!',
                                confirmButtonText: 'retry',
                                showCancelButton: true
                            }).then(function(){
                                semehr.search.easyLogin();
                            });
                            console.error('elasticsearch cluster is down!');
                        } else {
                            semehr.search._user_id = result[0];
                            swal({
                                title: 'Welcome back, ' + result[0] + "!",
                                confirmButtonText: 'ok',
                            });
                            console.log('All is well');
                        }
                    });
                }, function () {
                    swal.resetDefaults()
                })
            },

            setupES: function(){
                swal.setDefaults({
                    confirmButtonText: 'Next &rarr;',
                    showCancelButton: true,
                    animation: false,
                    progressSteps: ['1', '2']
                })

                var steps = [
                    {
                        title: 'SemEHR Elasticsearch Host',
                        text: 'host',
                        input: 'text',
                    },
                    {
                        title: 'SemEHR Elasticsearch Port',
                        text: 'port',
                        input: 'text'
                    },
                    {
                        title: 'XPack login',
                        text: 'Is there a xpack access/control in place? (yes, no)',
                        input: 'text',
                        confirmButtonText: "let's go"
                    }
                ]

                swal.queue(steps).then(function (result) {
                    swal.resetDefaults();
                    swal('setting up...');
                    swal.showLoading();
                    semehr.search.__es_server_url = result[0] + ":" + result[1];
                    semehr.search.__es_port = result[1];
                    semehr.search.__es_need_login = result[2] == 'yes';
                    swal.close();
                    semehr.search.doInit();
                }, function () {
                    swal.resetDefaults()
                })
            },

            queryPatient: function(queryBody, successCB, errorCB, from, size){
                if (!from) from = 0;
                if (!size) size = 10;
                var queryObj = {
                    index: semehr.search.__es_index,
                    type: semehr.search.__es_type,
                    body: queryBody,
                    from: from,
                    size: size
                };
                if (typeof queryBody == "string"){
                    queryObj = {
                        index: semehr.search.__es_index,
                        type: semehr.search.__es_type,
                        q: queryBody,
                        from: from,
                        size: size
                    };
                }
                semehr.search._es_client.search(queryObj).then(function (resp) {
                    var hits = resp.hits.hits;
                    if (hits.length > 0) {
                        var patients = [];
                        for(var i=0;i<hits.length;i++){
                            patients.push(semehr.search.getPatient(hits[i]));
                        }
                        successCB({"patients": patients, "total": resp.hits.total});
                    }else{
                        successCB({"patients": [], "total": 0})
                    }
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            queryDocuments: function(queryBody, srcFileds, size, successCB, errorCB, highlight){
                var queryObj = {
                    index: semehr.search.__es_fulltext_index[semehr.search.__indice_index],
                    type: semehr.search.__es_fulltext_type,
                    body: queryBody
                };
                if (typeof queryBody == "string"){
                    queryObj = {
                        index: semehr.search.__es_index,
                        type: semehr.search.__es_fulltext_type,
                        q: queryBody
                    };
                }
                if (srcFileds != null){
                    queryObj["_sourceInclude"] = srcFileds;
                }
                if (size != null){
                    queryObj["size"] = size;
                }
                if (highlight){
                    queryObj = {
                            index: semehr.search.__es_index,
                            type: semehr.search.__es_fulltext_type,
                            body:{
                                "query": {
                                    "query_string": {"query": queryObj.q}
                                },
                                "_source": [ "highlight" ],
                                "highlight":{
                                    "fields":{
                                    }
                                }
                            }                            
                        };
                    queryObj.body.highlight.fields[highlight] = {};
                }
                console.log(queryObj);
                semehr.search._es_client.search(queryObj).then(function (resp) {
                    var hits = resp.hits.hits;
                    console.log(resp.hits);
                    if (hits.length > 0) {
                        var docs = [];
                        for(var i=0;i<hits.length;i++){
                            docs.push(hits[i]);
                        }
                        successCB({"docs": docs, "total": resp.hits.total});
                    }else{
                        successCB({"docs": [], "total": 0})
                    }
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            queryPatientDocumentsMultiIndice: function(query, patientId, successCB, errorCB){
                var queryIndices = [];
                var textQs = [];
                var fulltextFields = [];
                for (var i=0;i<semehr.search.__es_fulltext_index.length;i++){
                    queryIndices.push(semehr.search.__es_fulltext_index[i]); 
                    textQs.push(semehr.search._full_text_attr[i]+ ":(" + query + ")");
                    fulltextFields.push(semehr.search._full_text_attr[i]);
                }

                var q = semehr.search._es_doc_patient_id_field + ":" + patientId + " AND (" + textQs.join(" OR ") + ")";
                var queryObj = {
                        index: queryIndices.join(","),
                        type: semehr.search.__es_fulltext_type,
                        body:{
                            "query": {
                                "query_string": {"query": q}
                            },
                            "_source": [ "highlight" ],                            
                            "highlight":{
                                "fields":{
                                }
                            }
                        },
                        size: 100                     
                    };
                for(var i=0;i<fulltextFields.length;i++)
                    queryObj.body.highlight.fields[fulltextFields[i]] = {
                        "fragment_size": 0,
                        "boundary_scanner_locale": "en"
                    };
                // console.log(queryObj);
                semehr.search._es_client.search(queryObj).then(function (resp) {
                    var hits = resp.hits.hits;
                    // console.log(resp.hits);
                    if (hits.length > 0) {
                        var docs = [];
                        for(var i=0;i<hits.length;i++){
                            docs.push(hits[i]);
                        }
                        successCB({"docs": docs, "total": resp.hits.total});
                    }else{
                        successCB({"docs": [], "total": 0})
                    }
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            searchConcept: function(search, successCB, errorCB, from, size){
                if (!from) from = 0;
                if (!size) size = 20;
                semehr.search._es_client.search({
                    index: semehr.search.__es_concept_index,
                    type: semehr.search.__es_concept_type,
                    q: search,
                    from: from,
                    size: size
                }).then(function (resp) {
                    var hits = resp.hits.hits;
                    if (hits.length > 0){
                        var ccs = [];
                        for(var i=0;i<hits.length;i++){
                            ccs.push(semehr.search.getContextedConcept(hits[i]));
                        }
                        successCB(ccs, resp.hits.total);
                    }else {
                        successCB([], 0);
                    }
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            searchPatientDocIds: function(queryBody, successCB, errorCB, from, size){
                if (!from) from = 0;
                if (!size) size = 20;
                var queryObj = {
                    index: semehr.search.__es_fulltext_index[semehr.search.__indice_index],
                    type: semehr.search.__es_fulltext_type,
                    body: queryBody
                };
                if (typeof queryBody == "string"){
                    queryObj = {
                        index: semehr.search.__es_fulltext_index[semehr.search.__indice_index],
                        type: semehr.search.__es_fulltext_type,
                        q: semehr.search._es_doc_patient_id_field + ":" + queryBody
                    };
                }
                queryObj["_source_includes"] = [];
                queryObj["_source_includes"] = queryObj["_source_includes"].concat(semehr.search._es_doc_type_field);
                queryObj["_source_includes"] = queryObj["_source_includes"].concat(semehr.search._es_doc_date_field);
                queryObj['from'] = from;
                queryObj['size'] = size;
                if (size != null){
                    queryObj["size"] = size;
                }
                semehr.search._es_client.search(queryObj).then(function (resp) {
                    var hits = resp.hits.hits;
                    // console.log(resp.hits);
                    if (hits.length > 0) {
                        var docs = [];
                        for(var i=0;i<hits.length;i++){
                            docs.push(hits[i]);
                        }
                        successCB({"docs": docs, "total": resp.hits.total});
                    }else{
                        successCB({"docs": [], "total": 0})
                    }
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            getConcept: function (ctxConceptId, successCB, errorCB) {
                semehr.search._es_client.get({
                    index: semehr.search.__es_concept_index,
                    type: semehr.search.__es_concept_type,
                    id: ctxConceptId
                }).then(function (resp) {
                    successCB(resp);
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            getDocument: function (docId, successCB, errorCB, trying) {
                if (!trying)
                    semehr.search.__indice_index = 0;
                semehr.search._es_client.get({
                    index: semehr.search.__es_fulltext_index[semehr.search.__indice_index],
                    type: semehr.search.__es_fulltext_type,
                    id: docId
                }).then(function (resp) {
                    successCB(resp);
                }, function (err) {
                    semehr.search.__indice_index++;
                    if (semehr.search.__indice_index < semehr.search.__es_fulltext_index.length){
                        console.log("trying "  + semehr.search.__es_fulltext_index[semehr.search.__indice_index]);
                        semehr.search.getDocument(docId, successCB, errorCB, true);
                    }
                    else{                        
                        errorCB(err);
                        console.trace(err.message);
                    }
                });
            },

            getPatient: function(hit){
                var annFields = hit["_source"]["anns"];
                var p = new semehr.Patient(hit["_id"]);
                var duplicate_ann_detector = {}; //do duplication check for multiple apperance of same annotations
                for(var i=0;i<annFields.length;i++){
                    //var app = annFields[i]["appearances"][0];
                    var annObj = annFields[i];
                    var uniqueAnnStr = annObj["cui"] + " " + annObj["eprid"] + " " + annObj["start"] + " " + annObj["end"];
                    if (uniqueAnnStr in duplicate_ann_detector)
                        continue;
                    var ann = new semehr.Annotation(
                        annFields[i]["contexted_concept"],
                        annFields[i]["cui"],
                        "sty" in annFields[i] ? annFields[i]["sty"] : "unknown_sty"
                    );
                    /*for(var j=0;j<annFields[i]["appearances"].length;j++){
                        var app = annFields[i]["appearances"][j];
                        ann.addAppearance(annObj["eprid"], annObj["start"], annObj["end"], '');
                    }*/
                    ann.addAppearance(annObj["eprid"], annObj["start"], annObj["end"], '', annObj["ruled_by"]);
                    p.addAnnotation(ann);
                    duplicate_ann_detector[uniqueAnnStr] = 1;
                }
                return p;
            },

            getContextedConcept: function(hit){
                var s = hit["_source"];
                return new semehr.ContextedConcept(hit["_id"], s["cui"],
                    s["prefLabel"], s["temporality"], s["negation"], s["experiencer"]);
            }
        };
    }
})(jQuery);
