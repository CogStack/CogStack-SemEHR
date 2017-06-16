/**
 * Created by honghan.wu on 22/04/2017.
 */
if (typeof semehr == "undefined"){
    var semehr = {};
}

(function($) {
    /**
     * Patient class
     * id - patient id
     * annId2Anns - a dictionary from annotation uid to annotation
     */
    if(typeof semehr.Patient == "undefined") {

        semehr.Patient = function(id){
            this.id = id;
            this.annId2Anns = {};
        };

        semehr.Patient.prototype.addAnnotation = function (ann) {
            if (ann.uid in this.annId2Anns){
                for(var i=0;i<ann.appearances.length;i++)
                    this.annId2Anns[ann.uid].addAppearanceInst(ann.appearances[i]);
            }else{
                this.annId2Anns[ann.uid] = ann;
            }
        };

        semehr.Patient.prototype.analyseMentions = function (concepts, validatedDocs) {
            var mentions = new semehr.AggMention("mentions");
            var otherMentions = {};
            var cui_check_str = concepts.join();
            var validated_doc_check_str = validatedDocs ? validatedDocs.join() : null;
            var cids = [];
            for (var cid in this.annId2Anns){
                var ann = this.annId2Anns[cid];
                if (cui_check_str.indexOf(ann.concept)>=0){
                    if (validated_doc_check_str && validated_doc_check_str.indexOf(ann.appearances[0]['eprid'])<0){
                        continue;
                    }
                    cids.push(cid);
                    mentions.addTypedFreq("allM", ann.appearances.length);
                    mentions.addAnnAppearance(ann.uid, ann.appearances);
                }else{
                    if (ann.concept in otherMentions){
                        otherMentions[ann.concept] += ann.appearances.length;
                    }else{
                        otherMentions[ann.concept] = ann.appearances.length;
                    }
                }
            }
            return {"mentions": mentions, "uniqueConcepts": cids, "otherConcepts": otherMentions};
        };
    }

    /**
     * Annotation class
     * uid - contextual concept unique id
     * concept - the umls concept id
     * appearances - a list of appearances of thie annotation
     */
    if(typeof semehr.Annotation == "undefined") {

        semehr.Annotation = function(uid, conceptId){
            this.uid = uid;
            this.concept = conceptId;
            this.appearances = [];
        };

        semehr.Annotation.prototype.addAppearance = function(doc, start, end, time){
            var app = {
                "doc": doc,
                "start": start,
                "end": end,
                "time": time
            };
            this.appearances.push(app);
        }

        semehr.Annotation.prototype.addAppearanceInst = function(app){
            this.appearances.push(app);
        }

    }

    if(typeof semehr.ContextedConcept == "undefined"){
        semehr.ContextedConcept = function(id, cui, prefLabel, temporality, negation, experiencer){
            this.id = id;
            this.concept = cui;
            this.label = prefLabel;
            this.temporality = temporality;
            this.negation = negation;
            this.experiencer = experiencer;
        }
    }
})(jQuery);
