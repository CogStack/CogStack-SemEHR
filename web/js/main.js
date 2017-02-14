(function($){
    var approved = null;
    var selection = null;
    var possibleConcept = null;

    function getUrlVars()
    {
        var vars = [], hash;
        var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
        for(var i = 0; i < hashes.length; i++)
        {
            hash = hashes[i].split('=');
            vars.push(hash[0]);
            vars[hash[0]] = hash[1];
        }
        return vars;
    }

    function loadDisorderConceptMappings(){
        qbb.inf.getDisorderConceptMappings("", function(s){
            // console.log(s);
            exact_map = $.parseJSON(s);
            loadApprovedData();
        });
    }

    function loadApprovedData(){
        qbb.inf.getDisorderMappings("", function(s){
            // console.log(s);
            approved = $.parseJSON(s);
            renderDisorders();
        });
    }

    function renderDisorders(){
        var s = "";
        var mapped = "";
        var not_mapped = "";
        var ordered = [];
        for (var d in exact_map){
            ordered.push(d);
        }
        ordered.sort();
        for (var i=0;i<ordered.length;i++){
            var d = ordered[i];
            r = "<div class='clsDisorder' concept=\""+ exact_map[d] + "\" disorder=\"" + d + "\">" +
                "<span class='clsApproved'>" + getApproveSymbol(d) + "</span>" +
                d + " -> " + (exact_map[d].length > 0 ? exact_map[d] : "N/A") + "</div>";
            if (exact_map[d].length > 0)
                mapped += r;
            else
                not_mapped += r;
        }
        s += mapped + not_mapped;
        $('#disoderList').html(s);

        $('.clsDisorder').click(function(){
            $('.clsSelected').removeClass('clsSelected');
            $(this).addClass('clsSelected');
            selection = $(this).attr('disorder');
            var url = "";
            if ($(this).attr('concept') && $(this).attr('concept').length > 0){
                url = "http://linkedlifedata.com/resource/umls/id/" + $(this).attr('concept');
                $( "#lldConcept" ).html('<object id="objView" width="100%" height="600px" data="' + url + '"/>');
            }
            else{
                var searchTerm = selection.replace("’", "'");
                url = "http://linkedlifedata.com/autocomplete.json?q=" + searchTerm + "&type=disorders";
                jQuery.ajax({
                							   type: "Get",
                							   url: url,
                							   data: [],
                							   cache: false,
                							   dataType: "jsonp",
                							   success: function(s){
                							       var ret = s;
                							       if (ret && ret.results && ret.results.length > 0
                							            && ret.results[0]['type'] == "Disorders"){
                							           var uri = ret.results[0]['uri'];
                							           var url = uri['namespace'] + uri['localName'];
                							           possibleConcept = uri['localName'];
                							           var so = {};
                							           so[selection] = possibleConcept;
                                                       qbb.inf.saveNewMappings($.toJSON(so), function(s){
                                                           if (s == 'ok'){
                                                               loadDisorderConceptMappings();
                                                           }else{
                                                               alert('mapping not saved');
                                                           }
                                                       });
                							           $( "#lldConcept" ).html('<object id="objView" width="100%" height="600px" data="' + url + '"/>');
                							       }else{
                							           $( "#lldConcept" ).html('not possible mappings found!');
                							       }
                							   },
                							   error: null
                						});
            }

            renderControl(selection);
        });
    }

    function getApproveSymbol(disorder){
        if (approved[disorder]){
            return approved[disorder] == "correct" ? "✓" : "✘";
        }else{
            return "-";
        }
    }

    function wrapControl(ctl, active){
        return "<span class='btn clsCtrl" + (active ? " clsActive" : "") + "'>" + ctl + "</span>";
    }

    function renderControl(disorder){
        var s = "";
        s += wrapControl("correct", approved[disorder] && approved[disorder] == "correct");
        s += wrapControl("wrong", approved[disorder] && approved[disorder] == "wrong");
        s += wrapControl("unapproved", !approved[disorder]);
        $( '#ctrlPanel' ).html(s);

        $('.clsCtrl').click(function(){
            if ($(this).html() == "unapproved"){
                approved[selection] = null;
            }else{
                var act = $(this).html();
                approved[selection] = act;
                var curConcept = $("div[disorder=\"" + selection + "\"]").attr('concept');
                if (act == "correct" && (!curConcept || curConcept.length == 0)){
                    console.log('save new mapping...' + possibleConcept + "..." + selection);
                }
            }
            console.log("save " + selection + " " + approved[selection]);
            renderControl(selection);
            $("div[disorder=\"" + selection + "\"] .clsApproved").html(getApproveSymbol(selection));
            var so = {};
            so[selection] = approved[selection];
            qbb.inf.saveDisOrderMapping($.toJSON(so), function(s){
                if (s != "ok"){
                    alert("saving action failed. please contact Honghan <honghan.wu@kcl.ac.uk>");
                }
                console.log('saved - ' + s);
            })
        });
        $('.clsText').click(function(){
            // console.log($('#objView').get(0).contentWindow.location.href);
//            alert($('#objView').attr('data'));
        });
    }
	$(document).ready(function(){
        var params = getUrlVars();
        if (params['new']){
            loadDisorderConceptMappings();
        }else{
            loadApprovedData();
        }
	})

})(this.jQuery)