(function($){
    var _invitationId = null;
    var _users = ['hepc_hw', 'hepc_gt', 'hepc_km'];
    var _user_feedback = {};
    var _display_attrs = ["charttime", "chartdate", "docType", "fulltext"];

    var _pageNum = 0;
    var _pageSize = 1;
    var _entityPageSize = 20;
    var _resultSize = 0;
    var _queryObj = null;
    var _prevQueryStr = null;
    var _currentDocMentions = null;

    var _umlsToHPO = {};

    var _context_concepts = null;
    var _cid2type = {};
    var _cohort = null;

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
        swal({"title": 'searching...', showConfirmButton: false})
        semehr.search.queryPatient(query_body, function(result){
            swal.resetDefaults();
            swal({title:"analysing...", showConfirmButton: false});
            console.log(result);
            if (result.total > 0) {
                summaris_cohort(result.patients, result.total);
            }else{
                $('#sumTermDiv').html('no records found');
            }
            swal.close();
        }, function(err){
            swal(err.message);
            console.trace(err.message);
        });
    }

    function cohortSearch(queryObj, cohorts, patientResults, currentOffset){
        var queryPatientSize = 2;
        if (currentOffset >= cohorts.length){
            swal.resetDefaults();
            swal({title:"analysing...", showConfirmButton: false});
            console.log(patientResults);
            if (patientResults.length > 0) {
                summaris_cohort(patientResults, patientResults.length);
            }else{
                $('#sumTermDiv').html('no records found');
            }
            swal.close();
        }else{
            var start = currentOffset;
            var end = Math.min(start + queryPatientSize, cohorts.length);
            currentOffset = end;
            var patientIds = cohorts.slice(start, end);
            _queryObj = queryObj;
            var termMaps = queryObj["terms"];
            var query_str = queryObj["query"];
            if (termMaps != null)
                query_str = termMaps.join(" ");
            var idConstrains = "";
            for (var i=0;i<patientIds.length;i++){
                idConstrains += " id:" + patientIds[i];
            }
            query_str += " AND (" + idConstrains + ")";
            //query_body["query"]["bool"]["must"].push( {match: {"id": entity_id}} );
            console.log(query_str);
            swal({"title": 'searching...', showConfirmButton: false})
            semehr.search.queryPatient(query_str, function(result){
                patientResults = patientResults.concat(result.patients);
                swal.resetDefaults();
                swal({title:"next batch search [" + currentOffset + "]...",
                    showConfirmButton: false});
                cohortSearch(queryObj, cohorts, patientResults, currentOffset);
            }, function(err){
                swal(err.message);
                console.trace(err.message);
            });
        }
    }

    function smartQuery(query){
        if (query.match(/(C\d{5,}\b)+/ig) || !$('#chkSearchConcept').prop('checked')){
            search({"terms": null, "query": query});
        }else{
            if (query == _prevQueryStr && $('.mappedCls:checked').length > 0){
                searchChecked();
            }else{
                _prevQueryStr = query;
                swal({title:"mapping concept...", showConfirmButton: false});
                var q = query + " AND temporality:Recent AND negation:Affirmed AND experiencer:Patient";
                console.log(q);
                semehr.search.searchConcept(q, function(concepts){
                    swal.close();
                    if (concepts.length <= 0){
                        $('#conceptMapDiv').html('no concepts found');
                    }else{
                        var s = "";
                        for(var i=0;i<concepts.length;i++){
                            s += "<div><input type='checkbox' class='mappedCls' id='chk_" + concepts[i].concept + "' value='" + concepts[i].concept + "'/><label for='chk_" + concepts[i].concept + "'>" + concepts[i].label + " (" + concepts[i].concept + ")</label></div>";
                        }
                        $('#conceptMapDiv').html(s);

                        $('.mappedCls:first').prop('checked', true);
                        searchChecked();
                    }
                }, function(err){
                    console.log(err);
                });
            }
        }
    }

    function searchChecked(){
        var terms = [];
        $('.mappedCls:checked').each(function(){
            terms.push($(this).val());
        });
        search({"terms": terms, "query": ""});
    }

    function summaris_cohort(entities, total){
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
            $('#sumTermDiv').append('<span class="btnCohort btnOtherView">concept analysis</span> <span class="btnCohort btnExport">export tsv</span>');
            $('.btnExport').click(function(){
                export_tsv();
            });
            
            $('.btnOtherView').click(function () {
                swal({title:'analysing concepts...', showConfirmButton: false});
                _cohort.getTopKOtherMentions(20, function(concepts, concept2label){
                    swal.close();
                    console.log(concepts);
                    barChartConceptFreq(concepts, concept2label);
                });
            });
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
    function highlight_text(anns, text, snippet, docId){
        var hos = [];
        for (var idx in anns){
            hos.push({"term": "", "s": anns[idx]['start'], "e": anns[idx]['end']});
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
        semehr.search.getDocument(docId, function(resp){
            var doc = {id: docId, mentions: doc2mentions[docId], docDetail: resp['_source']};
            renderDoc(doc);
            $('html, body').animate({
                scrollTop: $("#pageCtrl").offset().top
            }, 500);
        }, function(err){
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
            if (attr == "fulltext"){
                // val = "<span class='partial'>" + highlight_text(doc['mentions'], d[attr], true) + "</span>";
                val = "<span class='full'>" + highlight_text(doc["mentions"], d[attr], false, doc['id']) + "</span>";
                // val += "<span class='clsMore'>+</span>";
            }
            attrS += "<div class='clsField'>" + attr + "</div>";
            attrS += "<div attr='" + attr + "' class='clsValue'>" + val + "</div>";
            s += "<div class='clsRow clsDoc'>" + attrS + "</div>";
        }

        $('#results').html(s)

        for(var k in _user_feedback){
            $('#' + k + ' .' + _user_feedback[k]).addClass('fbed');
        }
        swal.close();
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

    $(document).ready(function(){
        semehr.search.initESClient();

        $('#btnSearch').click(function () {
            resetSearchResult();
            var q = $('#searchInput').val().trim();
            if (q.length == 0){
                swal({text:"please input your query", showConfirmButton: true});
            }else{
                smartQuery(q);
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

        $('#chkCohort').click(function() {
            if ($(this).prop('checked')){
                $("#cohortDiv").css('visibility', 'visible');
            }else{
                $("#cohortDiv").css('visibility', 'hidden');
            }
        });

        $('#chkValidDoc').click(function() {
            if ($(this).prop('checked')){
                $("#validatedDocDiv").css('visibility', 'visible');
            }else{
                $("#validatedDocDiv").css('visibility', 'hidden');
            }
        });
    })

})(this.jQuery)
