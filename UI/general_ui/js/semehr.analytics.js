/**
 * Created by honghan.wu on 22/04/2017.
 */
if (typeof semehr == "undefined"){
    var semehr = {};
}

(function($) {
    if(typeof semehr.Cohort == "undefined") {
        semehr.AggMention = function (name) {
            this.name = name;
            this.type2freq = {};
            this.uid2DocApp = {};
            this.typedDocApp = {};
        }

        semehr.AggMention.prototype.setTypedFreq = function(t, freq){
            this.type2freq[t] = freq;
        }

        semehr.AggMention.prototype.addTypedFreq = function(t, freq){
            if (t in this.type2freq)
                this.type2freq[t] = freq + this.type2freq[t];
            else
                this.type2freq[t] = freq;
        }

        semehr.AggMention.prototype.getTypedFreq = function(t){
            if (t in this.type2freq)
                return this.type2freq[t];
            else
                return 0;
        }

        semehr.AggMention.prototype.addAnnAppearance = function(uid, apps){
            var doc2app = {};
            var duplicate_detect_obj = {};
            for (var i=0;i<apps.length;i++){
                var app = apps[i];
                var key = app.doc + ' ' + app.start + ' ' + app.end;
                if (!(key in duplicate_detect_obj)){
                    duplicate_detect_obj[key] = 1;
                    if (apps[i].doc in doc2app){
                        doc2app[apps[i].doc].push(app);
                    }else{
                        doc2app[apps[i].doc] = [app];
                    }
                }
            }
            this.uid2DocApp[uid] = doc2app;
        }

        semehr.AggMention.prototype.addTypedDocApp = function(t, uid, docApp){
            if (!(t in this.typedDocApp))
                this.typedDocApp[t] = {};
            var uid2docApps = this.typedDocApp[t];
            if (!(uid in uid2docApps))
                uid2docApps[uid] = docApp;
        }

        semehr.AggMention.prototype.getTypedDocApps = function(t){
            if (t == "allM"){
                return this.uid2DocApp;
            }else
                return this.typedDocApp[t];
        }
    }

    /**
     * a cohort analysis class
     */
    if(typeof semehr.Cohort == "undefined") {

        semehr.Cohort = function(name){
            this.name = name;
            this.retrievedConcepts = {};
            this.typedconcepts = {};
            this.p2mentions = {};
            this.mergedOtherMentions = null;
        };

        semehr.Cohort.prototype.setPatients = function (patients) {
            this.patients = patients;
        };

        semehr.Cohort.prototype.assembleTypedData = function () {
            for(var i=0;i<this.patients.length;i++){
                var p = this.patients[i];
                var mentions = this.p2mentions[p.id]["mentions"];
                for(var cc in this.typedconcepts){
                    if (cc in p.annId2Anns){
                        mentions.addTypedFreq(this.typedconcepts[cc], p.annId2Anns[cc].appearances.length);
                        mentions.addTypedDocApp(this.typedconcepts[cc], cc, mentions.getTypedDocApps("allM")[cc]);
                    }
                }
            }
        };

        semehr.Cohort.prototype.summaryContextedConcepts = function(concepts, summCB){
            var p2mentions = {};
            var mergedUniqueConcepts = {};
            for(var i=0;i<this.patients.length;i++){
                var p = this.patients[i];
                var ret = p.analyseMentions(concepts);
                p2mentions[p.id] = ret;
                for(var j=0;j<ret["uniqueConcepts"].length;j++){
                    if (ret["uniqueConcepts"][j] in mergedUniqueConcepts){
                        mergedUniqueConcepts[ret["uniqueConcepts"][j]] += 1;
                    }else{
                        mergedUniqueConcepts[ret["uniqueConcepts"][j]] = 1;
                    }
                }
            }
            this.p2mentions = p2mentions;
            var cohort = this;
            var retrievedConcepts = this.retrievedConcepts;

            //call es api to get the relevant ctx_concepts detail
            for(var c in mergedUniqueConcepts) {
                semehr.search.getConcept(c, function(resp){
                    //console.log(resp);
                    retrievedConcepts[resp['_id']] = resp;
                    if (Object.keys(retrievedConcepts).length == Object.keys(mergedUniqueConcepts).length){
                        var cid2type = {};
                        // do typed analysis
                        for (var cid in retrievedConcepts){
                            var t = retrievedConcepts[cid];
                            if (t['_source']['experiencer'] == 'Patient'){
                                if (t['_source']['temporality'] != "Recent"){
                                    cid2type[cid] = 'hisM';
                                }else{
                                    if (t['_source']['negation'] == "Negated"){
                                        cid2type[cid] = 'negM';
                                    }else{
                                        cid2type[cid] = 'posM';
                                    }
                                }
                            }else{
                                cid2type[cid] = 'otherM';
                            }
                        }
                        cohort.typedconcepts = cid2type;
                        cohort.assembleTypedData();
                        summCB(true);
                    }
                }, function(err){
                    console.trace(err.message);
                });
            }
        };

        semehr.Cohort.prototype.getMergedOtherMentions = function(){
            if (this.mergedOtherMentions == null){
                this.mergedOtherMentions = {};
                var c2df = {};
                for(var p in this.p2mentions){
                    var om = this.p2mentions[p]["otherConcepts"];
                    for(var c in om){
                        c2df[c] = (c in c2df ? 1 + c2df[c] : 1);
                        this.mergedOtherMentions[c] = (c in this.mergedOtherMentions ?
                            this.mergedOtherMentions[c] + om[c] : om[c]);
                    }
                }
                for(var c in this.mergedOtherMentions){
                    this.mergedOtherMentions[c] = 1.0 * this.mergedOtherMentions[c] *
                        Math.log(Object.keys(this.p2mentions).length / c2df[c]);
                }
            }
            return this.mergedOtherMentions;
        }

        semehr.Cohort.prototype.getTopKOtherMentions = function(topK, cb){
            var oms = this.getMergedOtherMentions();
            var data = [];
            for (var c in oms){
                data.push({"concept": c, "freq": oms[c]});
            }
            data.sort(function (a, b) {
                return b.freq - a.freq;
            })
            var k = Math.min(topK, data.length);
            var topConcepts = data.slice(0, k);
            var concept2label = {};
            var counter = 0;
            for(var i=0;i<topConcepts.length;i++) {
                semehr.search.searchConcept(topConcepts[i]["concept"], function(ccs){
                    counter++;
                    if (ccs.length> 0 ){
                        concept2label[ccs[0]["concept"]] = ccs[0]["label"];
                    }
                    if (counter == topConcepts.length){
                        cb(topConcepts, concept2label);
                    }
                });
            }
        }
    }
})(jQuery);
