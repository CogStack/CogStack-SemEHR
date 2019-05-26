var _phenotypeSearches = {
	"HP:0000105": "\"enlarged kidneys\"~5 OR \"enlarged kidney\"~5",
	"HP:0005562": "AKPKD OR \"kidney cysts\"~5  OR \"polycystic kidney\"~5",
	"HP:0001407": "\"liver cysts\"~5",
	"HP:0001513": {"anchor": "bmi", "position": 1, "reg": /^\s{0,}([\:=]\s+){0,1}(\d+\.\d{0,5})/ig, "groupIndex": 2, "min": 25, "max": 1000},
	"HP:0000822": {"anchor": "bp", "position": 1, "reg": /^\s{0,}([\:=]\s+){0,1}(\d+\.\d{0,5})/ig, "groupIndex": 2, "min": 120, "max": 1000}
};

(function($) {
    if(typeof semehr.semsearch == "undefined") {
    	_highlight_begin_tag = "<em>";
    	_highlight_end_tag = "</em>";

        semehr.semsearch = {
        	getTextSearch: function(semObj){
        		if (typeof semObj == "string"){
        			return semObj;
        		}else{
        			return semObj.anchor;
        		}
        	},

        	getMatchedHTs: function(docs, semObj){
        		if (docs && docs.length == 0){
        			return [];
        		}
        		var matchedDocs = [];
    			for(var i=0;i<docs.length;i++){
    				var d = docs[i];
    				var matchedHTs = [];
                    if ('highlight' in d){
                        for(var k in d['highlight']){
                            var hts = d['highlight'][k];
                            for(var j=0;j<hts.length;j++){
                            	var ht = hts[j];
                            	if (typeof semObj == "string"){
				        			matchedHTs.push(ht);
				        		}else{
				        			var pos = ht.indexOf(_highlight_end_tag);
	                            	if (pos > 0){
	                            		pos += _highlight_end_tag.length;
	                            		if (semehr.semsearch.matchRegPattern(ht.substring(pos, ht.length), semObj)){
	                            			matchedHTs.push(ht);
	                            		}
	                            	}
				        		}
                            }
                        }
                    }
                    if (matchedHTs.length > 0){
                    	matchedDocs.push({"d": d['_id'], "hts": matchedHTs});
                    }
    			}
    			console.log('matched hts ' + matchedDocs);
    			return matchedDocs;
        	},

        	matchRegPattern: function(str, semObj){
        		var ptn = semObj.reg;
        		var m;
        		do {
				    m = ptn.exec(str);
				    if (m) {				    	
				        var v = parseFloat(m[semObj.groupIndex]);
				    	console.log('found: ' + m + " v:" + v);
				        if (v>= semObj.min && v <=semObj.max){
				        	return 1; //positive
				        }else{
				        	return -1; //negative
				        }
				    }
				} while (m);
				return 0; //unknown
        	}
        }
    }
})(jQuery);