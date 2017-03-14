(function($){
    var _es_client = null;
    var __es_server_url = "";
    var _display_attrs = ["client_idcode", "body_analysed"];

    var _pageNum = 0;
    var _pageSize = 25;
    var _resultSize = 0;
    var _queryObj = null;

    var _umlsToHPO = {};

    function search(queryObj){
        var termMaps = queryObj["terms"];
        var query_str = queryObj["query"];
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
                    shouldQuery.push({"match": {"yodie_ann.features.inst": termMaps[hpo][idx]}});
                }
                bq.push({bool: {should: shouldQuery}});
            }
            bq.minimum_should_match = 1;
        }
        if (query_str!=null && query_str.trim().length > 0){
            query_body["query"]["bool"]["must"].push( {match: {_all: query_str}} );
        }
		console.log(query_body);
        _es_client.search({
            index: 'epr_documents_bioyodie',
            type: 'semantic_anns',
            body: query_body
        }).then(function (resp) {
            var hits = resp.hits.hits;
            console.log(resp.hits);
            _resultSize = resp.hits.total;
            renderPageInfo();
            render_results(hits, termMaps);
        }, function (err) {
            console.trace(err.message);
        });
    }

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

    function highlight_text(anns, terms, text, snippet){
        var hos = [];
        for (var idx in anns){
            for (var hpo in terms){
				console.log(anns[idx]['features']['inst']);
				var umls_concepts = terms[hpo];
				for(var tidx in umls_concepts){
					if (anns[idx]['features']['inst'] == umls_concepts[tidx]){
						hos.push({"term": umls_concepts[tidx], "s": anns[idx]['startNode']['offset'], "e": anns[idx]['endNode']['offset']});
					}
				}
            }
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
                    "<em title='" + _umlsToHPO["UMLS:" + hos[idx]["term"]] + "'>" + text.substring(hos[idx]["s"], hos[idx]["e"]) + "</em>";
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

    function render_results(docs, terms){
        var attrs = _display_attrs;
        var html = "";
        var head = "";
        for (var idx in docs){
            var d = docs[idx]['_source'];
            var head = "<div class='clsField'>doc id</div>";
            var s = "<div attr='did' class='clsValue'>" + docs[idx]['_id'] + "</div>";
            for(var i=0;i<attrs.length;i++){
                var attr = attrs[i];
                var val = d[attr];
                if (attr == "body_analysed"){
                    val = "<span class='partial'>" + highlight_text(d["yodie_ann"], terms, d[attr], true) + "</span>";
                    val += "<span class='full'>" + highlight_text(d["yodie_ann"], terms, d[attr], false) + "</span>";
                    val += "<span class='clsMore'>+</span>";
                }
                head += "<div class='clsField'>" + attr + "</div>";
                s += "<div attr='" + attr + "' class='clsValue'>" + val + "</div>";
            }
            s = "<div class='clsRow clsDoc'>" + s + "</div>";
            html += s;
        }
        head = "<div class='clsRow'>" + head + "</div>" +
        $('#results').html(head + html);

        $('.clsMore').click(function () {
            var full = $(this).parent().find('.full');
            var patial = $(this).parent().find('.partial');
            if ($(full).is(":visible")){
                $(full).hide();
                $(patial).show();
                $(this).html('+');
            }else{
                $(full).show();
                $(patial).hide();
                $(this).html('-');
            }
        });
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

	$(document).ready(function(){
        genUMLSToHPO();

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

        $('#btnSearch').click(function () {
            _pageNum = 0;
            var q = $('#searchInput').val();
            if (q.trim().length == 0){
                swal({text:"please input your query", showConfirmButton: true});
            }else{
                _queryObj = getUMLSFromHPO(q.trim().split(" "))
                search(_queryObj);
            }
        });

        $('.clsNext').click(function () {
            if ($(this).hasClass("clsActive")){
                _pageNum++;
                search(_queryObj);
            }
        });

        $('.clsPrev').click(function () {
            if ($(this).hasClass("clsActive")){
                _pageNum--;
                search(_queryObj);
            }
        });


	})

})(this.jQuery)
