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
            this.pid2patient = {};
        };

        semehr.Cohort.prototype.setPatients = function (patients) {
            this.patients = patients;
            for (var i=0;i<patients.length;i++){
                this.pid2patient[patients[i].id] = patients[i];
            }
        };

        semehr.Cohort.prototype.getPatientById = function (id) {
            return this.pid2patient[id];
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

        semehr.Cohort.prototype.summaryContextedConcepts = function(concepts, summCB, validatedDocs){
            var p2mentions = {};
            var mergedUniqueConcepts = {};
            for(var i=0;i<this.patients.length;i++){
                var p = this.patients[i];
                var ret = p.analyseMentions(concepts, validatedDocs);
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

        semehr.Cohort.prototype.getSemanticTypedAnns = function(){
            var STY2Anns = {};
            for(var i=0;i<this.patients.length;i++){
                var p = this.patients[i];
                for (var uid in p.annId2Anns){
                    var sty = p.annId2Anns[uid].STY;
                    var cui = p.annId2Anns[uid].concept;
                    if (sty in STY2Anns){
                        var annDic = STY2Anns[sty];
                        if (cui in annDic){
                            annDic[cui] += p.annId2Anns[uid].appearances.length;
                        }else{
                            annDic[cui] = p.annId2Anns[uid].appearances.length;
                        }
                    }else{
                        STY2Anns[sty] = {};
                        STY2Anns[sty][cui] = p.annId2Anns[uid].appearances.length;
                    }
                }
            }

            // rank STYs
            var styList = [];
            for(var k in STY2Anns){
                styList.push({'s': k, 'n': Object.keys(STY2Anns[k]).length});
            }
            styList.sort(function(s1, s2){
                // return s2.n - s1.n;
                return s1.s >= s2.s ? 1 : -1;
            });
            console.log(STY2Anns);
            return [styList, STY2Anns];
        }
    }
})(jQuery);
