(function($){
    var __es_need_login = true;
    var _es_client = null;
    var __es_server_url = "http://timeline2016-silverash.rhcloud.com/";
    var __es_index = "pubmed"; //epr_documents_bioyodie
    var __es_type = "journal"; //semantic_anns
    var __es_concept_type = "ctx_concept";
    var __es_fulltext_index = "pubmed";
    var __es_fulltext_type = "doc";
    var _display_attrs = ["title", "fulltext"];
    var _full_text_attr = 'fulltext';
    var _fdid = 'pmcid';

    var _pageNum = 0;
    var _pageSize = 1;
    var _resultSize = 0;
    var _queryObj = null;
    var _currentDocMentions = null;

    var _umlsToHPO = {};

    var _context_concepts = null;

    function initESClient(){
        if (__es_need_login){
            easyLogin();
        }else{
            _es_client = new $.es.Client({
                hosts: __es_server_url
            });
            _es_client.ping({
                requestTimeout: 30000,
            }, function (error) {
                if (error) {
                    console.error('elasticsearch cluster is down!');
                } else {
                    console.log('All is well');
                }
            });
        }
    }

    function easyLogin(){
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
            _es_client = $.es.Client({
                host: [
                    {
                        host: __es_server_url,
                        auth: result[0] + ':' + result[1],
                        protocol: 'https',
                        port: 9200
                    }
                ]
            });
            _es_client.ping({
                requestTimeout: 30000,
            }, function (error) {
                if (error) {
                    swal({
                        title: 'something is wrong!',
                        confirmButtonText: 'retry',
                        showCancelButton: true
                    }).then(function(){
                        easyLogin();
                    });
                    console.error('elasticsearch cluster is down!');
                } else {
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
    }

    function search(queryObj){
        var termMaps = queryObj["terms"];
        var query_str = queryObj["query"];
        var entity_id = queryObj["entity"]
        var query_body = {
            from: _pageNum * _pageSize,
            size: _pageSize,
            query: {bool: {must:[]}}
        };
        if (termMaps!=null){
            var bq = query_body["query"]["bool"]["must"];
            for (var hpo in termMaps){
                var shouldQuery = [];
                for (var idx in termMaps[hpo]) {
                    shouldQuery.push({"match": {"_all": termMaps[hpo][idx]}});
                }
                bq.push({bool: {should: shouldQuery}});
            }
            bq.minimum_should_match = 1;
        }
        if (query_str!=null && query_str.trim().length > 0){
            query_body["query"]["bool"]["must"].push( {match: {"_all": query_str}} );
        }
        query_body["query"]["bool"]["must"].push( {match: {"id": entity_id}} );
		console.log(query_body);
		swal('searching...')
        _es_client.search({
            index: __es_index,
            type: __es_type,
            body: query_body
        }).then(function (resp) {
            swal('analysing...');
            var hits = resp.hits.hits;
            console.log(resp.hits);
            if (hits.length > 0) {
                summarise_entity_result(hits[0]);
            }else{
                $('#sumTermDiv').html('no records found');
            }
            swal.close();
            // _resultSize = resp.hits.total;
            // renderPageInfo();
            // render_results(hits, termMaps);
        }, function (err) {
            console.trace(err.message);
        });
    }

    function getTermDesc(umls_term, cb){
        _es_client.search({
            index: __es_index,
            type: __es_concept_type,
            q: umls_term
        }).then(function (resp) {
            var hits = resp.hits.hits;
            if (hits.length > 0 && cb)
                cb(hits[0]);
        }, function (err) {
            console.trace(err.message);
        });
    }

    /**
     * summarise the entity centric concept matchings
     *
     * @param entityObj
     */
    function summarise_entity_result(entityObj){
        $('#entitySumm').css("visibility", "visible");
        var summ_term = null;
        var cuis = [];
        if (Object.keys(_queryObj["terms"]).length > 0){
            for(var hp in _queryObj["terms"]) {
                summ_term = hp;
                cuis = cuis.concat(_queryObj["terms"][hp]);
            }
        }else {
            var keywords = _queryObj["query"].split(" ");
            for (var i=0;i<keywords.length;i++) {
                if (keywords[i].match(/C\d{5,}/ig)){
                    summ_term = keywords[i]
                    cuis.push(summ_term);
                }
            }
        }
        if (summ_term != null) {
            $('#sumTermDiv').html(summ_term);
            getTermDesc(cuis.join(' '), function(s){
                $('#sumTermDiv').html(s['_source']['prefLabel'] + "(" + summ_term + ")");
            });
        }else{
            sweetAlert('concept term not available')
        }
        var ctx_concepts = {};
        var ctx_to_freq = {};

        var totalM = 0;
        var cui_check_str = cuis.join();
        for(var i=0;i<entityObj['_source']['anns'].length;i++){
            var ann = entityObj['_source']['anns'][i];
            if (cui_check_str.indexOf(ann['CUI']) >= 0){
                var cc = ann['contexted_concept'];
                var doc2pos = {};
                totalM += ann['appearances'].length;
                ctx_to_freq[cc] = cc in ctx_to_freq ? ctx_to_freq[cc] + ann['appearances'].length : ann['appearances'].length;
                for (var j=0;j<ann['appearances'].length;j++){
                    if (ann['appearances'][j][_fdid] in doc2pos){
                        doc2pos[ann['appearances'][j][_fdid]].push(ann['appearances'][j]);
                    }else{
                        doc2pos[ann['appearances'][j][_fdid]] = [ann['appearances'][j]];
                    }
                }
                if (cc in ctx_concepts){
                    var exist_doc2pos = ctx_concepts[cc];
                    for (var d in doc2pos){
                        if (d in exist_doc2pos){
                            exist_doc2pos[d] = exist_doc2pos[d].concat(doc2pos[d]);
                        }else{
                            exist_doc2pos[d] = doc2pos[d];
                        }
                    }
                }else{
                    ctx_concepts[cc] = doc2pos;
                }
            }
        }
        _context_concepts = {'mentions': ctx_concepts, 'freqs':ctx_to_freq,
            'typed': {}, 'otherM': [], 'posM': [], 'negM':[], 'hisM': []};
        for(var c in ctx_concepts) {
            _es_client.get({
                index: __es_index,
                type: __es_concept_type,
                id: c
            }).then(function (resp) {
                console.log(resp);
                _context_concepts['typed'][resp['_id']] = resp;
                if (Object.keys(_context_concepts['typed']).length == Object.keys(_context_concepts['mentions']).length){
                    // do typed analysis
                    for (var cid in _context_concepts['typed']){
                        var t = _context_concepts['typed'][cid];
                        if (t['_source']['experiencer'] == 'Patient'){
                            if (t['_source']['temporality'] != "Recent"){
                                _context_concepts['hisM'].push(t);
                            }else{
                                if (t['_source']['negation'] == "Negated"){
                                    _context_concepts['negM'].push(t);
                                }else{
                                    _context_concepts['posM'].push(t);
                                }
                            }
                        }else{
                            _context_concepts['otherM'].push(t);
                        }
                    }

                    $('.posM').html(count_typed_freq('posM'));
                    $('.negM').html(count_typed_freq('negM'));
                    $('.otherM').html(count_typed_freq('otherM'));
                    $('.hisM').html(count_typed_freq('hisM'));
                }
            }, function (err) {
                console.trace(err.message);
            });
        }
        console.log(ctx_concepts);

        //render summarise result
        $('.allM').html(totalM);
    }

    function count_typed_freq(mentionType){
        var num_pos = 0;
        for (var i=0; i<_context_concepts[mentionType].length;i++){
            var t = _context_concepts[mentionType][i];
            num_pos += _context_concepts['freqs'][t['_id']];
        }
        return num_pos;
    }

    /**
     * calculate the fulltext annotation setting and then
     * call the rendering function to display the highlighted full
     * text
     *
     * @param ctx_concepts - the set of concepts to be rendered
     */
    function show_matched_docs(ctx_concepts){
        resetDocConceptCanvas();
        var doc2mentions = {};
        for (var cc in ctx_concepts){
            var cc_doc_mentions = ctx_concepts[cc];
            for(var d in cc_doc_mentions){
                if (d in doc2mentions){
                    doc2mentions[d] = doc2mentions[d].concat(cc_doc_mentions[d]);
                }else{
                    doc2mentions[d] = cc_doc_mentions[d];
                }
            }
        }
        _resultSize = Object.keys(doc2mentions).length;
        _currentDocMentions = doc2mentions;
        showCurrentPage();
    }

    /**
     * render current document fulltext with annotations highlighted
     */
    function showCurrentPage(){
        renderPageInfo();
        render_results(_currentDocMentions);
    }

    /**
     * render pagination controls
     */
    function renderPageInfo(){
        var totalPages = Math.floor(_resultSize / _pageSize) + (_resultSize % _pageSize == 0 ? 0 : 1);
        $('.clsPageInfo').html(_resultSize + " results, pages: " + (totalPages == 0 ? 0 : (_pageNum + 1) ) + "/" + totalPages);
        if (_pageNum + 1 < totalPages){
            $('.clsNext').addClass('clsActive');

        }else{
            $('.clsNext').removeClass('clsActive');
        }
        if (_pageNum > 0){
            $('.clsPrev').addClass('clsActive');
        }else{
            $('.clsPrev').removeClass('clsActive');
        }
        $('#pageCtrl').show();
    }

    /**
     * highlight fulltext with annotation metadata
     *
     * @param anns
     * @param text
     * @param snippet
     * @returns {string}
     */
    function highlight_text(anns, text, snippet){
        var hos = [];
        for (var idx in anns){
            hos.push({"term": "", "s": anns[idx]['offset_start'], "e": anns[idx]['offset_end']});
        }
        hos = hos.sort(function(a, b){
            return a["s"] - b["s"];
        });

        var moreTextLen = 20;
        var new_str = "";
        if (hos.length > 0){
            var prev_pos = snippet ? (hos[0]['s'] > moreTextLen ? hos[0]['s'] - moreTextLen : hos[0]['s']) : 0;
            if (prev_pos > 0)
                new_str += "...";
            for (var idx in hos){
                new_str += text.substring(prev_pos, hos[idx]["s"]) +
                    "<em>" + text.substring(hos[idx]["s"], hos[idx]["e"]) + "</em>";
                prev_pos = hos[idx]["e"];
                if (snippet)
                    break;
            }
            var endPos = snippet ? Math.min(parseInt(prev_pos) + moreTextLen, text.length) : text.length;
            new_str += text.substring(prev_pos, endPos);
            if (endPos < text.length)
                new_str += "...";
        }else{
            new_str = snippet ? text.substring(0, Math.min(text.length, moreTextLen)) + "...": text;
        }
        return new_str;
    }

    function render_results(doc2mentions){

        swal("loading documents...");
        var docs = Object.keys(doc2mentions);
        var docId = docs[_pageNum];

        _es_client.get({
            index: __es_fulltext_index,
            type: __es_fulltext_type,
            id: docId
        }).then(function (resp) {
            console.log(resp);
            var doc = {id: docId, mentions: doc2mentions[docId], docDetail: resp['_source']};
            renderDoc(doc);
        }, function (err) {
            console.trace(err.message);
        });


    }

    function renderDoc(doc){
        var attrs = _display_attrs;

        // var head = "<div class='clsField'>doc id</div>";
        var s =
            "<div class='clsRow'><div class='clsField'>DocID</div>" +
            "<div attr='did' class='clsValue'>" + doc['id'] + "</div></div>";
        var d = doc['docDetail'];
        for(var i=0;i<attrs.length;i++){
            var attrS = '';
            var attr = attrs[i];
            var val = d[attr];
            if (attr == _full_text_attr){
                // val = "<span class='partial'>" + highlight_text(doc['mentions'], d[attr], true) + "</span>";
                val = "<span class='full'>" + highlight_text(doc["mentions"], d[attr], false) + "</span>";
                // val += "<span class='clsMore'>+</span>";
            }
            attrS += "<div class='clsField'>" + attr + "</div>";
            attrS += "<div attr='" + attr + "' class='clsValue'>" + val + "</div>";
            s += "<div class='clsRow clsDoc'>" + attrS + "</div>";
        }

        $('#results').html(s)

        swal.close();
    }

    function getUMLSFromHPO(hpos){
        var mapped = {};
        var query = "";
        for (var idx in hpos){
            if (hpos[idx] in hpo_umls) {
                mapped[hpos[idx]] = [];
                for (var i in hpo_umls[hpos[idx]]){
                    mapped[hpos[idx]].push(hpo_umls[hpos[idx]][i].replace("UMLS:", ""));
                }
            }else {
                query += hpos[idx] + " ";
            }
        }
        return {terms: mapped, query: query};
    }

    function genUMLSToHPO(){
        for (var h in hpo_umls){
            for(var idx in hpo_umls[h]){
                _umlsToHPO[hpo_umls[h][idx]] = h;
            }
        }
    }

    function resetSearchResult(){
        $('#sumTermDiv').html('');
        $('#entitySumm').css("visibility", "hidden");
        _context_concepts = null;
        $('.sum').html('-');
        $('.sum').parent().removeClass('selected');
        resetDocConceptCanvas();
    }

    function resetDocConceptCanvas(){
        _pageNum = 0;
        _currentDocMentions = null;
        _resultSize = 0;
        $('#results').html('');
        $('#pageCtrl').hide();
    }

	$(document).ready(function(){
        genUMLSToHPO();
        initESClient();

        $('#btnSearch').click(function () {
            resetSearchResult();
            var q = $('#searchInput').val().trim();
            var entity = $('#entityInput').val().trim();
            if (q.length == 0 || entity.length == 0){
                swal({text:"please input your query", showConfirmButton: true});
            }else{
                _queryObj = getUMLSFromHPO(q.split(" "));
                _queryObj["entity"] = entity;
                search(_queryObj);
            }
        });

        $('.clsNext').click(function () {
            if ($(this).hasClass("clsActive")){
                _pageNum++;
                showCurrentPage();
            }
        });

        $('.clsPrev').click(function () {
            if ($(this).hasClass("clsActive")){
                _pageNum--;
                showCurrentPage();
            }
        });

        $('.sum').click(function(){
            if ($(this).hasClass('allM')){
                console.log('allM clicked');
                show_matched_docs(_context_concepts['mentions']);
            }else if ($(this).hasClass('posM')){
                console.log('posM clicked');
                var ctx_concept = {};
                for (var i=0;i<_context_concepts['posM'].length;i++){
                    var cc = _context_concepts['posM'][i]['_id'];
                    ctx_concept[cc] = _context_concepts['mentions'][cc];
                }

                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('negM')){
                console.log('negM clicked');
                var ctx_concept = {};
                for (var i=0;i<_context_concepts['negM'].length;i++){
                    var cc = _context_concepts['negM'][i]['_id'];
                    ctx_concept[cc] = _context_concepts['mentions'][cc];
                }

                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('hisM')){
                console.log('hisM clicked');
                var ctx_concept = {};
                for (var i=0;i<_context_concepts['hisM'].length;i++){
                    var cc = _context_concepts['hisM'][i]['_id'];
                    ctx_concept[cc] = _context_concepts['mentions'][cc];
                }

                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('otherM')){
                console.log('otherM clicked');
                var ctx_concept = {};
                for (var i=0;i<_context_concepts['otherM'].length;i++){
                    var cc = _context_concepts['otherM'][i]['_id'];
                    ctx_concept[cc] = _context_concepts['mentions'][cc];
                }

                show_matched_docs(ctx_concept);
            }
            $('.sum').parent().removeClass('selected');
            $(this).parent().addClass('selected');
        });

	})

})(this.jQuery)
