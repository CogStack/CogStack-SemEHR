(function($){
	var _user_feedback = {};
	var _performances = null;

	function getUserFeedback(){
        qbb.inf.getEvalResult(_invitationId, function(s){
        	swal.close();
            _user_feedback = $.parseJSON(s);
            console.log(_user_feedback);
			//render data
			render();
			//generate performance so far
			generatePerformanceChart();
        });
	}

	function generatePerformanceChart(){
		var c2p = {};
		for (var c in sample_docs){
			c2p[c] = {"posM": 0, "hisM": 0, "hypoM":0, "negM": 0, "otherM":0, "wrongM": 0};
			for(var i=0;i<sample_docs[c].length;i++){
				var docId = sample_docs[c][i]['id'];
				for (var j=0;j<sample_docs[c][i]["annotations"].length;j++){
					var annId = "d" + docId + "_s" + sample_docs[c][i]["annotations"][j]["start"] + "_e" + sample_docs[c][i]["annotations"][j]["end"];
					if (_user_feedback[annId]){
						c2p[c][_user_feedback[annId]] += 1;
					}
				}
			}
		}
		_performances = c2p;
		showPerformance();	
	}

	function exportPerformance(){
		var s = "";
		var headers = ["posM", "hisM", "hypoM", "negM", "otherM", "wrongM", "not validated"];
		s += "Term\t" + headers.join("\t") + "\n";
		for (var term in sample_docs){
			var r = [term];
			var validated = 0;
			for (var i=0;i<headers.length;i++){				
				if (headers[i] in _performances[term]){
					r.push((_performances[term][headers[i]] * 1.0 / sample_docs[term].length).toPrecision(2));
					validated += _performances[term][headers[i]];
				}
			}
			r.push(sample_docs[term].length - validated);

			s += r.join("\t") + "\n";
		}
		var w = window.open();
		$(w.document.body).html("<pre>" + s + "</pre>");
	}

	function showPerformance(){
		var term = $('#file_list').val();
		if (_performances){
			var docs = sample_docs[term];
			var p = _performances[$('#file_list').val()];
			var total = docs.length;
			if (total > 0){
				var numValidated = 0;
				var s = "";
				for (var k in p){
					s += k + ": " + (p[k]*1.0/total).toPrecision(2) + "(" + p[k] + ") ";
					numValidated += p[k];
				}
				s += "<br/> to be validated: " + (total - numValidated);
				$('.performanceDiv').html(s);
			}
		}
	}

	function preprocessDocContent(docContent){
		//bio-yodie seems having a bug in dealing with a long reptetitive empty spaces
		//therefore, doc content has been preprocessed before feeding to yodie. for this update,
		//we will need to preprocess accordingly before highlight
		if (_space_reduced){
			return docContent.replace(/(\s{2,})/ig, function(match, p1, string){
				if (p1.indexOf("\n") >= 0 )
					return "\n";
				else
					return " ";
			});
		}else
			return docContent;
	}

	function matchAnnInText(t, a, defaultIndex){
		if (!a)
			return -1;
		var reg = new RegExp("([\\s\\.;\\,$\\?!:/]|^)" + a + "([\\s\\.;\\,\\?!:/]|$)", 'ig');
		var matched = reg.exec(t);
		if (matched){
			var ret = [];
			do{
				var pos = matched.index + matched[1].length;
				ret.push([Math.abs(pos - defaultIndex), pos]);
			}while((matched = reg.exec(t))!= null)
			ret.sort(function(a, b){
				return a[0] - b[0];
			});
			console.log(ret);
			return ret[0][1];
		}else
			return -1;
	}

	function renderDoc(){
		var docs = getCurrentDocs();
		$('#pageInfo').html( '' + (index + 1) + '/' + docs.length);
		var html = docs[index].content;		
		var doContent = preprocessDocContent(docs[index].content);
		var inserts = [];
		for (var i=0;i<docs[index].annotations.length;i++){
			var docId = docs[index]["id"];
			//var start = docs[index].annotations[i]["string_orig"] ? doContent.toLowerCase().indexOf(" " + docs[index].annotations[i]["string_orig"].toLowerCase() + " ") : -1;
			var start = matchAnnInText(doContent, docs[index].annotations[i]["string_orig"], docs[index].annotations[i]["start"]);
			var end = start + docs[index].annotations[i]["string_orig"].length;
			console.log(docs[index].annotations[i]);
			if (start <0 ){
				start = docs[index].annotations[i]["start"];
				end = docs[index].annotations[i]["end"];
			}
			console.log(docs[index].annotations[i]["string_orig"] + ' ' + start + ' ' + end);
			inserts.push({"pos": start, "insert": "<span class='ann' title='" + docs[index].annotations[i]["concept"] + "'>"});
			var endStr = "</span>";
			endStr += "<span class='feedback' id='d" + docId + "_s" + docs[index].annotations[i]["start"] + "_e" + docs[index].annotations[i]["end"] + "'> <button class='fbBtn posM' title='positive mention'>posM</button> <button class='fbBtn hisM' title='history mention'>hisM</button> <button class='fbBtn hypoM' title='hypothetical mention'>hypoM</button> <button class='fbBtn negM' title='negative mention'>negM</button> <button class='fbBtn otherM' title='other experiencer mention'>otherM</button> <button class='fbBtn wrongM' title='this is NOT a mention'>wrongM</button></span>";
			inserts.push({"pos": end, "insert": endStr});
		}		
		inserts.sort(function(a, b){
			return a['pos'] - b['pos']
		});
		var prev_pos = 0;
		html = '[' + docs[index].id + ']' + docs[index].doc_table + ' - ' + docs[index].doc_col + '<p/>';
		for (var i=0;i<inserts.length;i++){
			html += doContent.substring(prev_pos, inserts[i]['pos']) + inserts[i]['insert'];
			prev_pos = inserts[i]['pos'];
		}
		html += doContent.substring(prev_pos);
		$('.docContainer').html(html.replace(/;;/g, '<p/>'));

		$('.fbBtn').click(function(event){
			event.preventDefault();
			swal('saving your feedback...');
            var annId = $(this).parent().attr('id');
            var data = {};
            var sel = $(this).html();
            data[annId] = sel;
            qbb.inf.saveEvalResult($.toJSON(data), _invitationId, function(s){
                if(s == 'true'){
                	swal.close();
                    swal('saved!');
                    $('#' + annId + ' button').removeClass('fbed');
                    $('#' + annId + ' .' + sel).addClass('fbed');
                    // rerender table to reflect the update
                    _user_feedback[annId] = sel;
                    renderDoc();
                }else{
                    swal('failed in saving!');
                }
            });
		});

		for(var k in _user_feedback){
            $('#' + k + ' .' + _user_feedback[k]).addClass('fbed');
		}
	}

	function renderDisorders(){
		var disorders = [];
		for(var c in sample_docs)
			disorders.push(c);
		if (typeof _term_freqs!=="undefined" && _term_freqs !== undefined){
			disorders = disorders.sort(function(a, b){
				var fa = isNaN(_term_freqs[a]) ? 0 : _term_freqs[a];
				var fb = isNaN(_term_freqs[b]) ? 0 : _term_freqs[b];
				return fb - fa;
			});
		}else
			disorders = disorders.sort();
		for(var i=0;i<disorders.length;i++){
			var c = disorders[i];
			var label = getTermLabel(c);
			$('#file_list').append($("<option></option>")
	                    .attr("value",c)
	                    .text(label)); 
		}
	}

	function getTermLabel(c){
		var label = c;
		if (typeof _term_freqs!=="undefined"){
			label += " [" + _term_freqs[c] + "] ";
		}
		if (typeof _dis_labels !== "undefined"){
			label = label + " (" + _dis_labels[c] + ")";
		}
		return label;
	}

	function render(){
		index = 0;
		var term = $('#file_list').val();
		$('.title').html(term);
		showPerformance();
		renderDoc();
	}

	function getCurrentDocs(){
		var td = sample_docs[$('#file_list').val()];
		var id2d = {};
		var d = null;
		var merged = [];
		for (var i=0;i<td.length;i++){
			if (!(td[i]["id"] in id2d)){
				d = {"id": td[i]["id"], "doc_table": td[i]["doc_table"], "doc_col": td[i]["doc_col"], "content": td[i]["content"], "annotations":[]};
				merged.push(d);
				id2d[td[i]["id"]] = d;
			}else{
				d = id2d[td[i]["id"]];
			}
			d["annotations"].push(td[i]["annotations"][0]);
			// for(var idx in td[i]["annotations"]){
			// 	if (td[i]["annotations"][idx]["start"]!=null)
			// 		console.log(td[i]["annotations"][idx])
			// }
		}
		return merged;
	}

	var index = 0;
	var data = null;
	$(document).ready(function(){
		renderDisorders();
		swal('loading...');
		getUserFeedback();
		$('#file_list option:eq(0)').prop('selected', true);
		data = sample_docs[$('#file_list').val()]

		$('#file_list').on('change', function() {
			data = sample_docs[this.value];
			render();
		});

		$( document ).keydown(function( event ) {
		  //event.preventDefault();
		  if (event.which == 39){
		  	index++;
		  	if (index >= getCurrentDocs().length)
		  		index = 0;
		  }
		  if (event.which == 37){
		  	index--;
		  	if (index < 0)
		  		index = getCurrentDocs().length - 1;
		  }
		  renderDoc();
		});

		$('.prevBtn').click(function(){
		  	index--;
		  	if (index < 0)
		  		index = getCurrentDocs().length - 1;
		  renderDoc();
		});
		$('.nextBtn').click(function(){
		  	index++;
		  	if (index >= getCurrentDocs().length)
		  		index = 0;
		  renderDoc();
		});

		$('#btnExport').click(function(){
			exportPerformance();
		});
	})

})(this.jQuery)