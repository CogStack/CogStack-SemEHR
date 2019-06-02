(function($){
    var _log_call_url = "./log_touch.html" // a local url to be called so that apache saves the log
    var _pageNum = 0;
    var _pageSize = 1;
    //entity pagination settings
    var _entityPageSize = 10;
    var _entityCurrentPage = 0;
    var _entityCurrentTotal = 0;
    var _entityCurrentQueryBody = null;

    var _resultSize = 0;
    var _queryObj = null;
    var _prevQueryStr = null;
    var _currentDocMentions = null;

    var _umlsToHPO = {};

    var _context_concepts = null;
    var _cid2type = {};
    var _cohort = null;
    var _curTypeConceptIndex = 0;

    var _curGeneralTypeConceptIndex = 0;
    var _curGeneralConceptSearch = null;

    var _sty2anns = null;
    var _sty2ontoMap = null;

    var _feedback = null;
    var _defaultFeedbackTemplate = "Phenotype result: \nComment:\n";

    var _patientResults = null;
    var _queriedPatientId = '';
    var _semsearch2do = 0;
    var __resultDivId = 'result';
    var __pageCtrlDivId = 'pageCtrl';
    function userLogin(){
        swal.setDefaults({
            confirmButtonText: 'Next &rarr;',
            showCancelButton: true,
            animation: false,
            progressSteps: ['1']
        })

        var steps = [
            {
                title: 'Login',
                text: 'name',
                input: 'text',
                confirmButtonText: 'login'
            }
        ]

        swal.queue(steps).then(function (result) {
            swal.resetDefaults();
            _invitationId = result[0];
            var matched = false;
            for(var i=0;i<_users.length;i++){
                if (_users[i] == _invitationId){
                    matched = true;
                    break;
                }
            }
            if (matched){
                swal('welcome ' + _invitationId + '!');
                $('#primaryNav').html('<span>' + _invitationId + '</span>');
                initESClient();
                // read user feedbacks from the server
                getUserFeedback();
            }else{
                _invitationId = null;
                swal('invalid user!');
            }
        });
    }

    function getUserFeedback(){
        qbb.inf.getEvalResult(_invitationId, function(s){
            _user_feedback = $.parseJSON(s);
        });
    }

    function search(queryObj){
        _queryObj = queryObj;
        if (doPatientFilter()){
            cohortSearch(queryObj, $('#cohortText').val().split(","), [], 0);
            return;
        }else{
            resetCohortConcepts();
        }
        var termMaps = queryObj["terms"];
        var query_str = queryObj["query"];
        var query_body = "";
        if (termMaps != null)
            query_str += " " + termMaps.join(" ");
        if (query_str!=null && query_str.trim().length > 0){
            query_body = query_str;
        }
        //query_body["query"]["bool"]["must"].push( {match: {"id": entity_id}} );
        console.log(query_body);
        _entityCurrentQueryBody = query_body;
        _entityCurrentPage = 0;
        showCurrentEntityPage();
    }

    function resetCohortConcepts(){
        $('#styListSpan').hide();
        $('#btnMoreSTY').hide();
        //$('#conceptMapDiv').§html('');
        $('#patientConceptMapDiv').html('');
        $('#diseasePhenotypeDiv').html('');
    }

    function cohortSearch(queryObj, cohorts, patientResults, currentOffset, totalPatientCount, givenTermMaps){
        var queryPatientSize = 2;
        if (currentOffset >= cohorts.length){
            swal.resetDefaults();
            swal({title:"analysing...", showConfirmButton: false});
            if (patientResults.length > 0) {
                if (!$('#patientConceptContainer .mappedCls:checked').length > 0){
                    cohortConcepts(patientResults);
                }
                summaris_cohort(patientResults, totalPatientCount);
                _entityCurrentTotal = totalPatientCount;
                _entityCurrentPage = 0;
                renderEntityPageInfo();
                swal.close();
            }else{
                $('#sumTermDiv').html('no records found');
                resetCohortConcepts();
                swal('patient not found!');
            }
        }else{
            var start = currentOffset;
            var end = Math.min(start + queryPatientSize, cohorts.length);
            currentOffset = end;
            var patientIds = cohorts.slice(start, end);

            var termMaps = [];
            if (!givenTermMaps){
                $('#patientConceptMapDiv .mappedCls:checked').each(function(){
                    termMaps.push($(this).val());
                });
            }else{
                termMaps = givenTermMaps;
            }
            var query_str = queryObj["query"];
            if (termMaps != null && termMaps.length > 0){
                query_str = "(" + termMaps.join(" ") + ")";
                queryObj['terms'] = termMaps;
            }

            _queryObj = queryObj;
            if (cohorts.length == 1 && cohorts[0] == ""){

            }else{
                var idConstrains = "";
                for (var i=0;i<patientIds.length;i++){
                    idConstrains += " id:" + patientIds[i];
                }
                query_str += " AND (" + idConstrains + ")";
            }

            //query_body["query"]["bool"]["must"].push( {match: {"id": entity_id}} );
            console.log(query_str);
            swal({"title": 'searching...', showConfirmButton: false})
            semehr.search.queryPatient(query_str, function(result){
                patientResults = patientResults.concat(result.patients);
                swal.resetDefaults();
                swal({title:"next batch search [" + currentOffset + "]...",
                    showConfirmButton: false});
                cohortSearch(queryObj, cohorts, patientResults, currentOffset, result.total, givenTermMaps);
            }, function(err){
                swal(err.message);
                console.trace(err.message);
            });
        }
    }

    function searchPatientDocs(pid, all_docs, from, size){
        var query_str = "" + pid;
        if (!from) from = 0;
        if (!size) size = 100;
        semehr.search.searchPatientDocIds(query_str, function(result){
            all_docs = all_docs.concat(result.docs);
            // console.log("got " + result.docs.length + " docs. total size: " + result.total);
            if (all_docs.length < result.total){
                // console.log('continuing searching from ' + (from + all_docs.length));
                searchPatientDocs(pid, all_docs, all_docs.length, size);
            }else{                
                semehr.Render.renderPatientDocs(all_docs, $('#docListContainer'), showDocDetail);
            }
        }, function(err){
            swal(err.message);
            console.trace(err.message);
        }, from, size);
    }

    function showDocDetail(docId){
        semehr.search.getDocument(docId, function(resp){
            var doc = {id: docId, mentions: [], docDetail: resp['_source']};
            var s = semehr.Render.getDocDisplay(doc);
            semehr.Render.showPopupLayer("<div id='docDetail'>" + s + "</div>");
        }, function(err){
            console.trace(err.message);
        });
    }

    function cohortConcepts(entities){
        var cohort = new semehr.Cohort("cohort");
        cohort.setPatients(entities);
        var rets = cohort.getSemanticTypedAnns();
        var styList = rets[0];
        var sty2anns = rets[1];
        $('#listSTY').find('option').remove();
        _sty2ontoMap = {};
        for(var i=0;i<styList.length;i++){
            var opt = document.createElement('option');
            opt.value = styList[i].s;
            opt.text =  styList[i].s + " (" +  styList[i].n + ")";
            _sty2ontoMap[styList[i].s] = styList[i].ontoMap;
            $('#listSTY').append(opt);
        }
        $('#styListSpan').show();
		$('#listSTY').val('HPO');
        _sty2anns = sty2anns;
//		renderTypedConcepts();
        renderDiseasePHO();
    }

    function populateDiseaseList(diseases){
        $('#listDisease').find('option').remove();
        $('#listDisease').html('<option value="">-select a disease model-</option>')
        _sty2ontoMap = {};
        for(var i=0;i<diseases.length;i++){
            var opt = document.createElement('option');
            opt.value = diseases[i];
            opt.text =  diseases[i];
            $('#listDisease').append(opt);
        }
    }

	function renderTypedConcepts(){
        $('#btnMoreSTY').hide();
        $('#btnMoreSTY').unbind('click');
	    var sty2anns = _sty2anns;
	    $('#patientConceptMapDiv').html('');
		var sty2annsFreq = sty2anns[$('#listSTY').val()];
        var anns = [];
        for(var k in sty2annsFreq){
            anns.push({'cui':k, 'freq': sty2annsFreq[k]});
        }
        anns.sort(function(a1, a2){
            return a2.freq - a1.freq;
        });

        var ontoMap = _sty2ontoMap[$('#listSTY').val()];

        //console.log(anns);
        $('#patientConceptMapDiv').html('');
        append_typed_concepts(anns, ontoMap);
//        append_typed_concepts(anns.slice(0, Math.min(20, anns.length)), ontoMap, 0);

        _curTypeConceptIndex = 20;

//        if (anns.length > 20){
//            $('#btnMoreSTY').show();
//            $('#btnMoreSTY').unbind('click');
//            $('#btnMoreSTY').click(function(){
//                if (_curTypeConceptIndex >= anns.length){
//                    $('#btnMoreSTY').hide();
//                }else{
//                    var ontoMap = _sty2ontoMap[$('#listSTY').val()];
//                    append_typed_concepts(anns.slice(_curTypeConceptIndex, Math.min(_curTypeConceptIndex + 20, anns.length)), ontoMap, _curTypeConceptIndex);
//                    $('html, body').animate({
//                        scrollTop: $("#lbl" + anns[_curTypeConceptIndex].cui).offset().top
//                    }, 1000);
//                    _curTypeConceptIndex += 20;
//                    if (_curTypeConceptIndex >= anns.length){
//                        $('#btnMoreSTY').hide();
//                    }
//                }
//            });
//        }
	}

	function append_typed_concepts(anns, ontoMap, filterStr){
	    var s = "";
        for(var i=0;i<anns.length;i++){
            var labelObj = ontoMap ? ontoMap[anns[i].cui] : anns[i].cui;
            var labels = [];
            if (!Array.isArray(labelObj)){
                labels = [labelObj];
            }else{
                labels = labelObj;
            }
            $.each(labels, function(index, label){
                if (!filterStr || filterStr.indexOf(label) < 0)
                    s += "<div cui='" + anns[i].cui + "' class='clsHPRow'><div class='clsHPCell clsFreq'>" + label + "</div><div class='clsHPCell clsLabel lbl" + anns[i].cui + "'></div><div class='clsHPCell clsFreq'>" + anns[i].freq + "</div></div>";
            });
        }
        $('#patientConceptMapDiv').append(s);

        for(var i=0;i<anns.length;i++ ){
            var labelObj = ontoMap ? ontoMap[anns[i].cui] : anns[i].cui;
            var labels = [];
            if (!Array.isArray(labelObj)){
                labels = [labelObj]
            }else{
                labels = labelObj
            }
            $.each(labels, function(index, label){
                if (!filterStr || filterStr.indexOf(label) < 0){
                    if ($('#lbl' + anns[i]['concept']).attr('labelRead') != 'yes'){
                        semehr.search.searchConcept(anns[i].cui, function(ctxConcepts){
                            var c = ctxConcepts[0];
                            $('.lbl' + c['concept']).attr('labelRead', 'yes');
                            $('.lbl' + c['concept']).html(c['label']);
                        }, function(){
                        });
                    }
                }
            });
        }

        $('.clsHPRow').unbind('click');
        $('.clsHPRow').click(function(){
            $('.searched').removeClass('searched');
            if ($(this).attr('cui') != ""){
                resetSearchResult();
                _queryObj["terms"] = [$(this).attr('cui')];
                popup_summaris_cohort(_patientResults, _entityCurrentTotal);
                _entityCurrentPage = 0;
                renderEntityPageInfo();
            }else{
                resetSearchResult();
            }
            $(this).addClass('searched');

            $('#pSumTermDiv').html(' HPO: ' + $(this).find('.clsLabel').html());
            $('#feedbackLabel').html('feedback for ' + _queriedPatientId + ' on ' + $(this).find('.clsLabel').html());
            // $('html, body').animate({
            //     scrollTop: $("#sumTermDiv").offset().top
            // }, 500);

            // put feedback in
            $('#feedBackText').val(_defaultFeedbackTemplate);
            if (_feedback){
                var feedbackKey = getFeedbackKey();
                if (feedbackKey in _feedback){
                    $('#feedBackText').val(_feedback[feedbackKey]);
                }
            }
        });
	}

    function smartQuery(query){
        if (doPatientFilter() && $('#conceptMapDiv .mappedCls:checked').length > 0){
            searchChecked();
        }else if (query.match(/(C\d{5,}\b)+/ig) || !$('#chkSearchConcept').prop('checked')){
            $('#conceptMapDiv').html('');
            search({"terms": null, "query": query});
        }else{
            if (query == _prevQueryStr && $('#conceptMapDiv .mappedCls:checked').length > 0){
                searchChecked();
            }else{
                _prevQueryStr = query;
                swal({title:"mapping concept...", showConfirmButton: false});
                var q = query + " AND temporality:Recent AND negation:Affirmed AND experiencer:Patient";
                console.log(q);
                _curGeneralTypeConceptIndex = 0;
                _curGeneralConceptSearch = q;
                $('#conceptMapDiv').html('');
                doSearchConcepts();
            }
        }
    }

    function doSearchConcepts(){
        var q = _curGeneralConceptSearch;
        if ($('#chkSearchConcept').prop('checked')){
            if ($('#listGeneralSTY').val() != "*")
                q += " AND STY:" + $('#listGeneralSTY').val();
        }

        semehr.search.searchConcept(q, function(concepts, totalConcepts){
            swal.close();
            if (concepts.length <= 0){
                $('#conceptMapDiv').html('no concepts found');
            }else{
                var s = "";
                for(var i=0;i<concepts.length;i++){
                    s += "<div><input type='checkbox' class='mappedCls' id='chk_" + concepts[i].concept + "' value='" + concepts[i].concept + "'/><label for='chk_" + concepts[i].concept + "'>" + concepts[i].label + " (" + concepts[i].concept + ")</label></div>";
                }
                $('#conceptMapDiv').append(s);

                // $('.mappedCls:first').prop('checked', true);
                // searchChecked();

                _curGeneralTypeConceptIndex += concepts.length;

                if (totalConcepts > _curGeneralTypeConceptIndex){
                    $('#btnGeneralMoreSTY').show();
                }else{
                    $('#btnGeneralMoreSTY').hide();
                }
            }
        }, function(err){
            console.log(err);
        }, _curGeneralTypeConceptIndex, 20);
    }

    function searchChecked(){
        var terms = [];
        $('#conceptMapDiv .mappedCls:checked').each(function(){
            terms.push($(this).val());
        });
        search({"terms": terms, "query": ""});
    }

    function summaris_cohort(entities, total){
        _patientResults = entities;
        $('#entitySummHeader').css("visibility", "visible");
        $('#dataRowDiv').show();
        $('#entitySumm').css("visibility", "visible");
        var summ_term = null;
        var cuis = [];
        if (_queryObj["terms"] && _queryObj["terms"].length > 0){
            cuis = _queryObj["terms"];
        }else {
            var keywords = _queryObj["query"].split(" ");
            for (var i=0;i<keywords.length;i++) {
                if (keywords[i].match(/C\d{5,}/ig)){
                    summ_term = keywords[i]
                    cuis.push(summ_term);
                }
            }
        }
        $('#sumTermDiv').html(entities.length + " of " + total + " matched patients");

        _context_concepts = {
            'mentions': {},
            'freqs':{},
            'typed': {},
            'entityMentions': {},
            'typedFreqs': {}
        };
        entities = entities.sort(function(a, b){
            return a['_id'] - b['_id'];
        });
        for (var i=0;i<entities.length;i++){
            //summarise_entity_result(entities[i], cuis);
            var entityObj = entities[i];

            $('#entitySumm').append($('#sumRowTemplate').html());
            var row = $('#entitySumm .sumRow:last');
            $(row).attr('id', "r" + entityObj.id);
            $('#r' + entityObj.id + " .patientId").html(entityObj.id);
        }

        var cohort = new semehr.Cohort("cohort");
        var validatedDocs = null;
        if ($('#chkValidDoc').prop('checked') && $('#validatedDocText').val().length>0){
            validatedDocs = $('#validatedDocText').val().split(',');
        }
        cohort.setPatients(entities);
        cohort.summaryContextedConcepts(cuis, function(){
            _cid2type = cohort.typedconcepts;
            renderSumTable(true);
//            $('#sumTermDiv').append('<span class="btnCohort btnOtherView">concept analysis</span> <span class="btnCohort btnExport">export tsv</span>');
//            $('.btnExport').click(function(){
//                export_tsv();
//            });
//
//            $('.btnOtherView').click(function () {
//                swal({title:'analysing concepts...', showConfirmButton: false});
//                _cohort.getTopKOtherMentions(20, function(concepts, concept2label){
//                    swal.close();
//                    console.log(concepts);
//                    barChartConceptFreq(concepts, concept2label);
//                });
//            });
        }, validatedDocs);
        _cohort = cohort;


        $('.sum').click(function(){
            if ($(this).html() == '-')
                return;
            var entityId = $(this).attr('entityId');
            var m = _cohort.p2mentions[entityId]["mentions"];
            if ($(this).hasClass('allM')){
                //console.log(_context_concepts['entityMentions'][entityId]['all']);
                show_matched_docs(m.getTypedDocApps('allM'));
            }else if ($(this).hasClass('posM')){
                var ctx_concept = m.getTypedDocApps('posM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('negM')){
                var ctx_concept = m.getTypedDocApps('negM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('hisM')){
                var ctx_concept = m.getTypedDocApps('hisM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('otherM')){
                var ctx_concept = m.getTypedDocApps('otherM');
                show_matched_docs(ctx_concept);
            }
            $('.sum').parent().removeClass('selected');
            $(this).parent().addClass('selected');
        });

        $('.patientId').click(function(){
            var pid = $(this).html();
            var p = _cohort.getPatientById(pid);
            swal({"title": 'summarising patient...', showConfirmButton: false});
            p.summarise(function(sum){
                swal.close();
                //get terms from query object
                var cuis = _queryObj.terms;
                if (cuis == null){
                    cuis = [];
                    var m = null;
                    var re = /(C\d{5,})+/ig
                    do {
                        var m = re.exec(_queryObj.query);
                        if (m) {
                            cuis.push(m[1]);
                        }
                    } while (m);
                }
                semehr.Render.renderSummaries(sum, cuis);
            });
        });
    }

    function popup_summaris_cohort(entities, total){
        var s = '';
        _patientResults = entities;
        var summ_term = null;
        var cuis = [];
        if (_queryObj["terms"] && _queryObj["terms"].length > 0){
            cuis = _queryObj["terms"];
        }else {
            var keywords = _queryObj["query"].split(" ");
            for (var i=0;i<keywords.length;i++) {
                if (keywords[i].match(/C\d{5,}/ig)){
                    summ_term = keywords[i]
                    cuis.push(summ_term);
                }
            }
        }
        s += "<div id='pSumTermDiv'></div>";
        s += "<div id='pEntitySumm'>"
            + $('#entitySumm').html()
            + $('#sumRowTemplate').html()
            + "</div>";
        s += "<div id='pPageCtrl'>" + $('#pageCtrlTemplate').html() + "</div>"
        s += "<div id='pResults'></div>";
        __resultDivId = 'pResults';
        __pageCtrlDivId = 'pPageCtrl';

        _context_concepts = {
            'mentions': {},
            'freqs':{},
            'typed': {},
            'entityMentions': {},
            'typedFreqs': {}
        };
        var entityObj = entities[0]; // only one entity
        semehr.Render.showPopupLayer(s);
        $('#pEntitySumm').css('visibility', 'visible');
        $('#pEntitySumm .headerRow').css('visibility', 'visible');

        var row = $('#pEntitySumm .sumRow:last');
        $(row).attr('id', "r" + entityObj.id);
        $('#r' + entityObj.id + " .patientId").html(entityObj.id);

        var cohort = new semehr.Cohort("cohort");
        var validatedDocs = null;
        if ($('#chkValidDoc').prop('checked') && $('#validatedDocText').val().length>0){
            validatedDocs = $('#validatedDocText').val().split(',');
        }
        cohort.setPatients(entities);
        cohort.summaryContextedConcepts(cuis, function(){
            _cid2type = cohort.typedconcepts;
            renderSumTable(true);
        }, validatedDocs);
        _cohort = cohort;


        $('.sum').click(function(){
            if ($(this).html() == '-')
                return;
            var entityId = $(this).attr('entityId');
            var m = _cohort.p2mentions[entityId]["mentions"];
            if ($(this).hasClass('allM')){
                //console.log(_context_concepts['entityMentions'][entityId]['all']);
                show_matched_docs(m.getTypedDocApps('allM'));
            }else if ($(this).hasClass('posM')){
                var ctx_concept = m.getTypedDocApps('posM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('negM')){
                var ctx_concept = m.getTypedDocApps('negM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('hisM')){
                var ctx_concept = m.getTypedDocApps('hisM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('otherM')){
                var ctx_concept = m.getTypedDocApps('otherM');
                show_matched_docs(ctx_concept);
            }else if ($(this).hasClass('ruledM')){
                var ctx_concept = m.getTypedDocApps('ruled');
                show_matched_docs(ctx_concept);
            }
            $('.sum').parent().removeClass('selected');
            $(this).parent().addClass('selected');
        });

        $('.clsNext').unbind('click');
        $('.clsPrev').unbind('click');
        $('.clsEntityNext').unbind('click');
        $('.clsEntityPrev').unbind('click');
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


        $('.clsEntityNext').click(function () {
            if ($(this).hasClass("clsActive")){
                _entityCurrentPage++;
                showCurrentEntityPage();
            }
        });

        $('.clsEntityPrev').click(function () {
            if ($(this).hasClass("clsActive")){
                _entityCurrentPage--;
                showCurrentEntityPage();
            }
        });
    }

    function renderSumTable(){
        for(var entityId in _cohort.p2mentions){
            var entityMention = _cohort.p2mentions[entityId]["mentions"];
            var row = '#r' + entityId;
            $(row).find('.sum').attr('entityId', entityId);
            $(row).find('.patientId').html(entityId);
            $(row).find('.allM').html(entityMention.getTypedFreq('allM'));

            if (entityMention.getTypedFreq('posM') > 0){
                $(row).find('.posM').html(entityMention.getTypedFreq('posM'));
            }
            if (entityMention.getTypedFreq('negM') > 0){
                $(row).find('.negM').html(entityMention.getTypedFreq('negM'));
            }

            if (entityMention.getTypedFreq('otherM') > 0){
                $(row).find('.otherM').html(entityMention.getTypedFreq('otherM'));
            }

            if (entityMention.getTypedFreq('hisM') > 0){
                $(row).find('.hisM').html(entityMention.getTypedFreq('hisM'));
            }

            if (entityMention.getTypedFreq('ruled') > 0){
                $(row).find('.ruledM').html(entityMention.getTypedFreq('ruled'));
            }
        }
    }

    function export_tsv(){
        var w = window.open();
        var html = '';
        var header = ['Patient ID', 'Total Mentions', 'Positive Mentions', 'History/hypothetical Mentions', 'Negative Mentions', 'Other Experiencers'];
        html += header.join('\t') + '\n';
        for(var entityId in _cohort.p2mentions){
            var row = [entityId];
            var entityMention = _cohort.p2mentions[entityId]["mentions"];
            row.push(entityMention.getTypedFreq('allM'));
            row.push(entityMention.getTypedFreq('posM'));
            row.push(entityMention.getTypedFreq('hisM'));
            row.push(entityMention.getTypedFreq('negM'));
            row.push(entityMention.getTypedFreq('otherM'));
            html += row.join('\t') + '\n';
        }
        html = '<pre>' + html + '</pre>';
        $(w.document.body).html(html);
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
     * render fulltext doc pagination controls
     */
    function renderPageInfo(){
        var totalPages = Math.floor(_resultSize / _pageSize) + (_resultSize % _pageSize == 0 ? 0 : 1);
        $('#' + __pageCtrlDivId + ' .clsPageInfo').html(_resultSize + " results, pages: " + (totalPages == 0 ? 0 : (_pageNum + 1) ) + "/" + totalPages);
        if (_pageNum + 1 < totalPages){
            $('#' + __pageCtrlDivId + ' .clsNext').addClass('clsActive');

        }else{
            $('#' + __pageCtrlDivId + ' .clsNext').removeClass('clsActive');
        }
        if (_pageNum > 0){
            $('#' + __pageCtrlDivId + ' .clsPrev').addClass('clsActive');
        }else{
            $('#' + __pageCtrlDivId + ' .clsPrev').removeClass('clsActive');
        }
        $('#' + __pageCtrlDivId).show();
    }


    /**
     * render current entity search result page
     */
    function showCurrentEntityPage(){
        resetSearchResult();
        swal({"title": 'searching...', showConfirmButton: false})
        semehr.search.queryPatient(_entityCurrentQueryBody, function(result){
            swal.resetDefaults();
            swal({title:"analysing...", showConfirmButton: false});
            console.log(result);
            if (result.total > 0) {
                _entityCurrentTotal = result.total;
                renderEntityPageInfo();
                summaris_cohort(result.patients, result.total);
            }else{
                $('#sumTermDiv').html('no records found');
            }
            swal.close();
        }, function(err){
            swal(err.message);
            console.trace(err.message);
        }, _entityCurrentPage * _entityPageSize, _entityPageSize);
    }

    /**
     * render entity pagination controls
     */
    function renderEntityPageInfo(){
        var totalPages = Math.floor(_entityCurrentTotal / _entityPageSize) + (_entityCurrentTotal / _entityPageSize == 0 ? 0 : 1);
        $('.clsEntityPageInfo').html(_entityCurrentTotal + " results, pages: " + (totalPages == 0 ? 0 : (_entityCurrentPage + 1) ) + "/" + totalPages);
        if (_entityCurrentPage + 1 < totalPages){
            $('.clsEntityNext').addClass('clsActive');

        }else{
            $('.clsEntityNext').removeClass('clsActive');
        }
        if (_entityCurrentPage > 0){
            $('.clsEntityPrev').addClass('clsActive');
        }else{
            $('.clsEntityPrev').removeClass('clsActive');
        }
//        $('#entityPaginationDiv').show();
    }

    function render_results(doc2mentions){

        swal("loading documents...");
        var docs = Object.keys(doc2mentions);
        var docId = docs[_pageNum];
        semehr.search.getDocument(docId, function(resp){
            var doc = {id: docId, mentions: doc2mentions[docId], docDetail: resp['_source']};
            semehr.Render.renderDoc(doc, $('#' + __resultDivId));
            // $('html, body').animate({
            //     scrollTop: $("#pageCtrl").offset().top
            // }, 500);
        }, function(err){
            console.trace(err.message);
        });

    }

    function barChartConceptFreq(data, item2label){
        $('#chartDiv').html('');
        $('#chartDivOverlay').css('visibility', 'visible');
        $("#chartDivOverlay").show();
        d3.select("#chartDiv").append("div")
            .attr("id", "chartToolTip")
            .attr("class", "tooltip");
        d3.select("#chartDiv").append("svg")
            .attr("width", + $('#chartDiv').width())
            .attr("height", + $('#chartDiv').height() - $('#chartToolTip').height());
        $('#chartDiv').append('<label class="modal__close"></label>');
        $('.modal__close').click(function(){

            $("#chartDivOverlay").hide();
        });
        $('#chartToolTip').html('top ' + data.length + ' concepts (other than you searched)');

        var svg = d3.select("svg"),
            margin = {top: 20, right: 20, bottom: 30, left: 40},
            width = +svg.attr("width") - margin.left - margin.right,
            height = +svg.attr("height") - margin.top - margin.bottom;

        var x = d3.scaleBand().rangeRound([0, width]).padding(0.3),
            y = d3.scaleLinear().rangeRound([height, 0]);

        var g = svg.append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        x.domain(data.map(function(d) { return d.concept; }));
        y.domain([0, d3.max(data, function(d) { return d.freq; })]);

        g.append("g")
            .attr("class", "axis axis--x")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x));

        g.append("g")
            .attr("class", "axis axis--y")
            .call(d3.axisLeft(y).ticks(10))
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", "0.71em")
            .attr("text-anchor", "end")
            .text("Frequency");

        var div = d3.select("#chartToolTip")
            .attr("class", "tooltip");

        g.selectAll(".bar")
            .data(data)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function(d) { return x(d.concept); })
            .attr("y", function(d) {
                return y(+d.freq);
            })
            .attr("width", x.bandwidth())
            .attr("height", function(d) { return height - y(d.freq); })
            .attr("data-tooltip", function(d) { return item2label[d.concept] + ' (' + d.concept + ')' + ":"  + d.freq; })
            .on("mouseover", function(d) {
                div.transition()
                    .duration(200)
                    .style("opacity", .9);
                div	.html(item2label[d.concept] + ' (' + d.concept + ')' + ":"  + d.freq)
                    // .style("left", (d3.event.pageX) + "px")
                    // .style("top", (d3.event.pageY - 28) + "px");
            })
            .on("mouseout", function(d) {
                // div.transition()
                //     .duration(500)
                //     .style("opacity", 0);
            });
        d3.select("#chartDivOverlay").style("opacity", 1)
    }

    function resetSearchResult(){
        $('#sumTermDiv').html('');
        $('#entitySummHeader').css("visibility", "hidden");
        $('#dataRowDiv').hide();
        $('#entitySumm').css("visibility", "hidden");
        _context_concepts = null;
        _cid2type = {};
        $('#entitySumm').find('.dataRow').remove();
        resetDocConceptCanvas();
    }

    function resetDocConceptCanvas(){
        _pageNum = 0;
        _currentDocMentions = null;
        _resultSize = 0;
        $('#results').html('');
        $('#pageCtrl').hide();
    }

    function doPatientFilter(){
        return $('#chkCohort').prop('checked') && $('#cohortText').val().length > 0;
    }

    function renderDiseasePHO(){
        if (_sty2anns == null)
            return;
        $('#diseasePhenotypeDiv').html('');
        //get patient hpo dict
        var patientHPO = {};
        var sty2anns = _sty2anns;
        var voc = 'HPO';
        var sty2annsFreq = sty2anns[voc];
        var ontoMap = _sty2ontoMap[voc];
        var anns = [];
        if (ontoMap){
            for(var k in sty2annsFreq){
                if (ontoMap[k]){
                    $.each(ontoMap[k], function(index, hpo){
                        patientHPO[hpo] = {'freq': sty2annsFreq[k], 'cui': k};
                    });
                }
                anns.push({'cui':k, 'freq': sty2annsFreq[k]});
            }
        }
        anns.sort(function(a1, a2){
            return a2.freq - a1.freq;
        });

        var strHPOs = "";
        var disObj = disease_model[$('#listDisease').val()];
        if (disObj){
            strHPOs = render_typed_concepts(disObj, patientHPO, _phenotypeSearches, complementarySearch4EmptyPhenotypes);
        }
        $('#patientConceptMapDiv').html('');
        append_typed_concepts(anns, ontoMap, strHPOs);
        retrieveFeedback();
    }

    function render_typed_concepts(disObj, patientHPO, phenoSearches, doSearchFunc){
	    var s = "";
	    var strHPOs = "";
        var searches = [];
        for(var i=0;i<disObj.length;i++){
            if ("hpo_id" in disObj[i]){
                var phenotype = disObj[i]['hpo_id'];
                strHPOs += disObj[i]['hpo_id'] + ";";
                s += "<div cui='" + (disObj[i]['hpo_id'] in patientHPO ?  patientHPO[phenotype]['cui'] : '' ) + "' class='clsHPRow" + (phenotype in patientHPO ?  '' : ' clsNotMatched' ) + "'><div class='clsHPCell clsFreq'>" + phenotype + 
                "</div><div class='clsHPCell clsLabel' id='lbl" + phenotype + "'>" + disObj[i]['hpo_label'] + "</div><div class='clsHPCell clsFreq clsNum'>" +
                (phenotype in patientHPO ?  patientHPO[phenotype]['freq'] : '-' )+ "</div></div>";

                if (!(phenotype in patientHPO)){
                    if (phenotype in phenoSearches){
                        searches.push({'q': phenoSearches[phenotype], 'pid': _queriedPatientId, 'phenotype': phenotype});
                    }
                }
            }
        }
        $('#diseasePhenotypeDiv').html(s);

        if (searches.length > 0 && doSearchFunc){
            swal.close();
            _semsearch2do = searches.length;
            swal("doing semantic searching for " + searches.length + " phenotypes...")
            doSearchFunc(searches);
        }

        return strHPOs;
	}

    function complementarySearch4EmptyPhenotypes(searches){
        /*
        * use phenotype semantic queries to complement unknown phenotypes
        */
        $(searches).each(function(index, qo){
            semehr.search.queryPatientDocumentsMultiIndice(semehr.semsearch.getTextSearch(qo.q), qo.pid, function(resp){
                _semsearch2do--;
                if (_semsearch2do <= 0){
                    swal.close();
                }
                var mDocs = semehr.semsearch.getMatchedHTs(resp['docs'], qo.q);
                if (mDocs && mDocs.length > 0){
                    var p = qo.phenotype.replace(/\:/, "\\:");
                    $('#lbl' + p).parent().removeClass('clsNotMatched');
                    $('#lbl' + p).parent().addClass('clsSearchMatched');
                    $('#lbl' + p).parent().find(".clsNum").html('±' + mDocs.length);
                    semehr.Render.renderSemQueryResult(p, mDocs);        
                }

            }, function(err){
                console.trace(err.message);
            });
        });
    }

	function retrieveFeedback(){
	    qbb.inf.getEvalResult(_invitationId, function(res){
//	        console.log('retrieved:' + res);
            _feedback = $.parseJSON(res);
	    });
	}

	function getFeedbackKey(){
	    var hpoId = $($('.searched').find('.clsFreq').get(0)).html();
        return _queriedPatientId + "_" + hpoId;
	}

    $(document).ready(function(){
        semehr.search.initESClient();
        getUserFeedback();

        $('#btnSearch').click(function () {
            resetSearchResult();
            var q = $('#searchInput').val().trim();
            if (q.length == 0){
                swal({text:"please input your query", showConfirmButton: true});
            }else{
                $.ajax({
                    url: _log_call_url,
                    data: {q: q, u: semehr.search._user_id},
                    success: function(s){console.log(s)}
                });
                smartQuery(q);
            }
        });

        $('#btnEntitySearch').click(function () {
            resetSearchResult();
            var q = $('#searchEntityInput').val().trim();
            if (q.length == 0){
                swal({text:"please input your query", showConfirmButton: true});
            }if ($.trim($('#cohortText').val()).length == 0){
                swal({text:"please add patient id", showConfirmButton: true});
            }
            else{
                var patientIds = $('#cohortText').val().split(",");
                _queriedPatientId = $('#cohortText').val()
                $('#feedbackLabel').html('feedback for ' + _queriedPatientId);
                $.ajax({
                    url: _log_call_url,
                    data: {q: q, u: semehr.search._user_id},
                    success: function(s){console.log(s)}
                });
                cohortSearch({"terms": null, "query": q}, patientIds, [], 0, patientIds.length);
                var allDocs = [];
                searchPatientDocs(patientIds[0], allDocs);
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


         $('.clsEntityNext').click(function () {
            if ($(this).hasClass("clsActive")){
                _entityCurrentPage++;
                showCurrentEntityPage();
            }
        });

        $('.clsEntityPrev').click(function () {
            if ($(this).hasClass("clsActive")){
                _entityCurrentPage--;
                showCurrentEntityPage();
            }
        });

        $('#chkCohort').click(function() {
            if ($(this).prop('checked')){
                $("#cohortDiv").show();
            }else{
                $("#cohortDiv").hide();
            }
        });

        $('#chkValidDoc').click(function() {
            if ($(this).prop('checked')){
                $("#validatedDocDiv").show()
            }else{
                $("#validatedDocDiv").hide();
            }
        });
        $('#chkCohort').click();


        $('#btnGeneralMoreSTY').click(function(){
            doSearchConcepts();
        });

        $('#chkSearchConcept').click(function() {
            $("#styGeneralListSpan").toggle(this.checked);
        });

        $('.tabTitle').click(function(){
            $('.semTab').hide();
            if ($(this).html() == 'CONCEPT'){
                $('#tabConcept').show();
            }
            if ($(this).html() == 'PATIENT'){
                $('#tabEntity').show();
            }
            $('.tabTitle').removeClass('tabSelected');
            $(this).addClass('tabSelected');
        });

        $('.tabView').click(function(){
            $('.viewContent').hide();
            if ($(this).html() == 'PHENOTYPES'){
                $('#phenotypeView').show();
            }
            if ($(this).html() == 'DOCUMENTS'){
                $('#patientDocumentView').show();
            }
            $('.tabView').removeClass('tabSelected');
            $(this).addClass('tabSelected');
        });


		$('#styListSpan').on('change', function() {
		  renderTypedConcepts();
		});

		var diseases = [];
		for (var d in disease_model){
		    diseases.push(d);
		}
		populateDiseaseList(diseases.sort());
		$('#listDisease').on('change', function() {
		  renderDiseasePHO();
		});

		$('#btnSaveFeedback').click(function(){
		    if ($('#feedBackText').val().length == 0){
		        swal({text:"please input your feedback", showConfirmButton: true});
		    }else{
                var feedbackItemId = getFeedbackKey();
                var feedback = {};
                feedback[feedbackItemId] = $('#feedBackText').val();
                console.log($.toJSON(feedback));
                swal({title:"saving feedback...", showConfirmButton: false});
                qbb.inf.saveEvalResult($.toJSON(feedback), _invitationId, function(res){
                    if (res == 'true'){
                        retrieveFeedback();
                        swal('saved');
                    }else{
                        swal('NOT saved! something is wrong!');
                    }
                });
		    }
		});
    })

})(this.jQuery)
