(function($){
    var approved = null;
    var selection = null;

    function loadApprovedData(){
        qbb.inf.getDisorderMappings("", function(s){
            console.log(s);
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
            r = "<div class='clsDisorder' concept='"+ exact_map[d] + "' disorder='" + d + "'>" +
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
            var url = "http://linkedlifedata.com/resource/umls/id/" + $(this).attr('concept');
            $( "#lldConcept" ).html('<object width="100%" height="600px" data="' + url + '"/>');
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
                approved[selection] = $(this).html();
            }
            console.log("save " + selection + " " + approved[selection]);
            renderControl(selection);
            $("div[disorder='" + selection + "'] .clsApproved").html(getApproveSymbol(selection));
            var so = {};
            so[selection] = approved[selection];
            console.log(so);
            qbb.inf.saveDisOrderMapping($.toJSON(so), function(s){
                if (s != "ok"){
                    alert("saving action failed. please contact Honghan <honghan.wu@kcl.ac.uk>");
                }
                console.log('saved - ' + s);
            })
        });
    }
	$(document).ready(function(){
	    loadApprovedData();
	})

})(this.jQuery)