/**
 * Created by honghan.wu on 21/04/2017.
 */
if (typeof semehr == "undefined"){
    var semehr = {};
}

(function($) {
    if(typeof semehr.search == "undefined") {

        semehr.search = {
            __es_need_login: false,
            _es_client: null,
            __es_server_url: "http://10.200.102.23:9200/",
            __es_index: "mimic", //epr_documents_bioyodie
            __es_concept_index: "mimic", //concept index for separation of ES6
            __es_type: "patient", //patient type
            __es_concept_type: "ctx_concept",
            __es_fulltext_index: "mimic",
            __es_fulltext_type: "eprdoc",
            _full_text_attr: 'fulltext',
            _user_id: "not_logged",
            _fdid: '_id',
            __discharge_summary_type: "Discharge summary",

            initESClient: function(){
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
                                port: 9200
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

            queryPatient: function(queryBody, successCB, errorCB){
                var queryObj = {
                    index: semehr.search.__es_index,
                    type: semehr.search.__es_type,
                    body: queryBody
                };
                if (typeof queryBody == "string"){
                    queryObj = {
                        index: semehr.search.__es_index,
                        type: semehr.search.__es_type,
                        q: queryBody
                    };
                }
                semehr.search._es_client.search(queryObj).then(function (resp) {
                    var hits = resp.hits.hits;
                    console.log(resp.hits);
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

            queryDocuments: function(queryBody, srcFileds, size, successCB, errorCB){
                var queryObj = {
                    index: semehr.search.__es_index,
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

            searchConcept: function(search, successCB, errorCB){
                semehr.search._es_client.search({
                    index: semehr.search.__es_concept_index,
                    type: semehr.search.__es_concept_type,
                    q: search,
                    size: 20
                }).then(function (resp) {
                    var hits = resp.hits.hits;
                    if (hits.length > 0){
                        var ccs = [];
                        for(var i=0;i<hits.length;i++){
                            ccs.push(semehr.search.getContextedConcept(hits[i]));
                        }
                        successCB(ccs);
                    }else {
                        successCB([]);
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

            getDocument: function (docId, successCB, errorCB) {
                semehr.search._es_client.get({
                    index: semehr.search.__es_fulltext_index,
                    type: semehr.search.__es_fulltext_type,
                    id: docId
                }).then(function (resp) {
                    successCB(resp);
                }, function (err) {
                    errorCB(err);
                    console.trace(err.message);
                });
            },

            getPatient: function(hit){
                var annFields = hit["_source"]["anns"];
                var p = new semehr.Patient(hit["_id"]);
                for(var i=0;i<annFields.length;i++){
                    var ann = new semehr.Annotation(
                        annFields[i]["contexted_concept"],
                        annFields[i]["CUI"]
                    );
                    for(var j=0;j<annFields[i]["appearances"].length;j++){
                        var app = annFields[i]["appearances"][j];
                        ann.addAppearance(app["eprid"], app["offset_start"], app["offset_end"], app["date"]);
                    }
                    p.addAnnotation(ann);
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
