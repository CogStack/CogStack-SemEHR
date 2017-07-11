/**
 * Created by honghan.wu on 07/07/2017.
 */
if (typeof semehr == "undefined"){
    var semehr = {};
}

(function($) {
    if (typeof semehr.MedProfile == "undefined") {

        semehr.MedProfile = {
            headerPattern: /^([^\n\:]+)\:.*$/img,
            normalisePattern: /^\d\.\s+.*/ig,
            numberPattern: /(\d+%)|(\d+[,\.\/]*\d+)/,
            sectsWithMeasures: ["Vital Signs", "Hospital course", "Hospital Discharge Studies", "pertinent results",
                "Hospital Discharge Physical"],
            annTypeNeedMeasures: ["Tissue", "Laboratory Procedure", "Pharmacologic Substance", "Health Care Activity"],
            maxMeasureDistance: 15,

            parseSumStruct: function(s){
                var re = semehr.MedProfile.headerPattern;
                var m;
                var sections = [];
                do {
                    m = re.exec(s);
                    if (m) {
                        sections.push({"section": m[1], "pos": [m.index, m.index + m[0].length]});
                    }
                } while (m);
                return sections;
            },

            normaliseText: function(s){
                return s.trim().toLowerCase().replace(semehr.MedProfile.normalisePattern, '');
            },

            putAnnsIntoSection: function(full_text, prev_pos, sec, sec_pos, anns, start_index, container,
                                         original_sec, start_offset){
                anns = anns.sort(function(a1, a2){
                   return a1['startNode']['offset'] - a2['startNode']['offset'];
                });
                var checked_ann_idx = start_index;
                var sec_obj = {'section': sec, 'anns': [],
                    'original_section': original_sec, 'start':prev_pos, 'end': sec_pos};
                sec_obj['text'] = full_text.substring(prev_pos,sec_pos);
                for(var i=start_index;i<anns.length;i++){
                    if (anns[i]['startNode']['offset'] < sec_pos){
                        if (start_offset < anns[i]['startNode']['offset'])
                            sec_obj['anns'].push(anns[i]);
                    }
                    else{
                        checked_ann_idx = i>0? i - 1 : i;
                        break;
                    }
                }

                container.push(sec_obj);
                return checked_ann_idx;
            },

            generateMedProfile: function(fullText, anns){
                var sections = semehr.MedProfile.parseSumStruct(fullText);
                console.log(sections);

                var checked_ann_idx = 0;
                var prev_sec = '';
                var prev_orig_sec = '';
                var prev_pos = 0;
                var structured_summary = [];
                var prev_start_offset = 0;
                for (var i=0;i<sections.length;i++){
                    var s = sections[i];
                    var normalised_sec = semehr.MedProfile.normaliseText(s['section'])
                    var FHIR_Sec = mimicFHIRMapping[normalised_sec] ? mimicFHIRMapping[normalised_sec] : null;
                    if (FHIR_Sec != null){
                        checked_ann_idx = semehr.MedProfile.putAnnsIntoSection(
                            fullText, prev_pos, prev_sec, s['pos'][0], anns,
                            checked_ann_idx,
                            structured_summary,
                            prev_orig_sec,
                            prev_start_offset);
                        prev_sec = FHIR_Sec;
                        prev_orig_sec = s['section'];
                        prev_pos = s['pos'][0];
                        prev_start_offset = s['pos'][1];
                        console.log(FHIR_Sec + " " +  s['pos']);
                    }

                }

                if (sections.length > 0)
                    semehr.MedProfile.putAnnsIntoSection(fullText, prev_pos,
                        prev_sec, fullText.length, anns, checked_ann_idx, structured_summary,
                        prev_orig_sec, 0);

                console.log(structured_summary);
                return structured_summary;
            },

            parseLabMeasurements: function(text){
                var re = semehr.MedProfile.numberPattern;
                var m = re.exec(text);
                if (m){
                    return {"value": m[1] ? m[1] : m[2], "pos": m.index};
                }
            },

            parseMedicalSection: function(structuredSum){
                var measures = {};
                for (var i=0;i<structuredSum.length;i++){
                    var sec = structuredSum[i];
                    if (jQuery.inArray(sec.section, semehr.MedProfile.sectsWithMeasures) >= 0){
                        for(var j=0;j<sec.anns.length;j++){
                            var ann = sec.anns[j];
                            if (jQuery.inArray(ann.features.STY, semehr.MedProfile.annTypeNeedMeasures) >= 0){
                                var annEndPos = ann.endNode.offset - sec.start;
                                var mo = semehr.MedProfile.parseLabMeasurements(sec.text.substring(annEndPos));
                                if (mo && mo.pos < semehr.MedProfile.maxMeasureDistance){
                                    if (ann.features.PREF in measures){
                                        measures[ann.features.PREF].value.push(mo.value);
                                    }else
                                        measures[ann.features.PREF] = {"value": [mo.value], "cui": ann.features.inst};
                                }
                            }
                        }
                    }
                }
                return measures;
            }
        };
    }
})(jQuery);