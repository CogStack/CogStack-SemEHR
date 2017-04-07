if (typeof qbb == "undefined"){
	var qbb = {};
}

(function($) {
	if(typeof qbb.inf == "undefined") {

		qbb.inf = {
			service_url: "http://54.218.95.198:8080/fbrain/api",

			saveDisOrderMapping: function(map, searchCB){
				var apiName = "saveDisOrderMapping";
				var sendObject={
						r:apiName,
						map: map
				};
				qbb.inf.callAPI(sendObject, searchCB);
			},

            saveNewMappings: function(map, searchCB){
                var apiName = "saveNewMappings";
                var sendObject={
                    r:apiName,
                    map: map
                };
                qbb.inf.callAPI(sendObject, searchCB);
            },

			getDisorderMappings: function(url, searchCB){
				var apiName = "getDisorderMappings";
				var sendObject={
						r:apiName,
						url: url
				};
				qbb.inf.callAPI(sendObject, searchCB);
			},

            getDisorderConceptMappings: function(url, searchCB){
                var apiName = "getDisorderConceptMappings";
                var sendObject={
                    r:apiName,
                    url: url
                };
                qbb.inf.callAPI(sendObject, searchCB);
            },

			callAPI: function(sendObject, cb){
				qbb.inf.ajax.doPost(sendObject, function(s){
					var ret = s;
					if (ret && ret.status == "200" && ret.data)
					{
						if (typeof cb == 'function')
							cb(ret.data);
					}else
					{
						if (typeof cb == 'function')
							cb();
					}
				}, function(){
					if (typeof checkOutDataCB == 'function')checkOutDataCB();
				});
			},

			ajax: {
					doGet:function(sendData,success,error){
						qbb.inf.ajax.doSend("Get",null,sendData,success,error);
					},
					doPost:function(sendData,success,error){
						qbb.inf.ajax.doSend("Post",null,sendData,success,error);
					},
					doSend:function(method,url,sendData,success,error){
						dataSuccess = function(data){
							(success)(eval(data));
						};
						if (sendData) sendData.token = "";
						jQuery.ajax({
							   type: method || "Get",
							   url: url || qbb.inf.service_url,
							   data: sendData || [],
							   cache: false,
							   dataType: "jsonp", /* use "html" for HTML, use "json" for non-HTML */
							   success: dataSuccess /* (data, textStatus, jqXHR) */ || null,
							   error: error /* (jqXHR, textStatus, errorThrown) */ || null
						});
					}
			}
		};
	}
})(jQuery);