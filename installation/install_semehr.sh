#!/bin/bash

script_path=$PWD

read -p "please specify the full path to install semehr:" install_dir


if [ -d ${install_dir} ]; then
 echo "using installation folder ${install_dir}"
else
 echo "folder ${install_dir} not exists"
 exit 1
fi

# ignore this as the cloud storage issue has been resolved
# if [ ! -e "${install_dir}/gcp/bio-yodie-1-2-1" ]; then
#  echo "please download bio-yodie from the given URL and unzip it to ${install_dir}/gcp/bio-yodie-1-2-1"
#  exit 1
# fi

# install apps
sudo apt-get install -y \
	ant \
	curl \
	openjdk-11-jdk \
	subversion \
	unzip \
	vim \
	git

JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

cd ${install_dir}
mkdir ./gcp
cd ./gcp
JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8'

## gcp
if [ ! -e "${install_dir}/gcp/gcp-2.5-18658" ]; then
 curl -L 'https://knowlab.github.io/data/gcp-2.5.hw.tar.gz' > gcp-2.5.hw.tar.gz && tar -xzvf gcp-2.5.hw.tar.gz
 cd ${install_dir}/gcp/gcp-2.5-18658/lib
 curl -L 'https://knowlab.github.io/data/customised_handlers.tar.gz' > customised_handlers.tar.gz && tar xzvf customised_handlers.tar.gz && cp customised_handlers/* ./ && rm -fr customised_handlers && rm -f customised_handlers.tar.gz
else
 echo "gcp exists, skip"
fi
GCP_HOME="${install_dir}/gcp/gcp-2.5-18658"

## gate
cd ${install_dir}/gcp
if [ ! -e "${install_dir}/gcp/gate" ]; then
 curl -L 'http://netix.dl.sourceforge.net/project/gate/gate/8.1/gate-8.1-build5169-ALL.zip' > gate-8.1-build5169-ALL.zip && unzip gate-8.1-build5169-ALL.zip && mv gate-8.1-build5169-ALL gate && rm gate-8.1-build5169-ALL.zip
else
 echo "gate exists, skip"
fi 

GATE_HOME="${install_dir}/gcp/gate"
PATH="$PATH:$GCP_HOME:$GATE_HOME/bin"

## bioyodie
cd "${install_dir}/gcp"
if [ ! -e "${install_dir}/gcp/bio-yodie-1-2-1" ]; then
 curl -L 'https://www.homepages.ucl.ac.uk/~rmhihww/semehr_tools/bio-yodie-1.2.1-se.tar.gz' > bio-yodie-1.2.1-se.tar.gz && tar xzvf bio-yodie-1.2.1-se.tar.gz && rm bio-yodie-1.2.1-se.tar.gz && mv bio-yodie-1.2.1 bio-yodie-1-2-1
else
 echo "bio-yodie exists, skip"
fi


# install semehr github repo and dependencies
sudo apt-get install -y \
    python3 \
    python3-pip \
    python-setuptools \
    python-dev

sudo apt-get install -y python3-testresources

mkdir "${install_dir}"/semehr
cd "${install_dir}"/semehr
if [ ! -e "${install_dir}"/semehr/CogStack-SemEHR ]; then
 git clone -b safehaven_mini https://github.com/CogStack/CogStack-SemEHR.git
 cd CogStack-SemEHR
else
 echo 'reop exists'
 cd CogStack-SemEHR
 git pull
fi
pip3 install -r requirements.txt

## mk data folder
if [ ! -e ${install_dir}/data ]; then
 mkdir ${install_dir}/data
 mkdir ${install_dir}/data/input_docs
 mkdir ${install_dir}/data/output_docs
 mkdir ${install_dir}/data/semehr_results
fi

## semehr config 
conf_template_file="$script_path/semehr_conf_template.json"
echo "setup SemEHR configuration file"
if [ ! -e ${conf_template_file} ]; then
 echo "[error] ${conf_template_file} not exists"
 exit 2
fi


if [ ! -e "${install_dir}/data/semehr_settings.json" ]; then
 cp $script_path/semehr_conf_template.json ${install_dir}/data/semehr_settings.json
 sed -i -e "s@PH_JAVA_HOME@$JAVA_HOME@g" ${install_dir}/data/semehr_settings.json
 sed -i -e "s@PH_SEMEHR_INSTALL_PATH@$install_dir@g" ${install_dir}/data/semehr_settings.json
else
 echo "${install_dir}/data/semehr_settings.json exists, skip"
fi

# install nlp2phenome
if [ ! -e "${install_dir}"/data/phenome_results ]; then
 mkdir "${install_dir}"/data/phenome_results
fi
cd "${install_dir}"/semehr
if [ ! -e "${install_dir}"/semehr/nlp2phenome ]; then
 git clone -b safehaven_mini https://github.com/CogStack/nlp2phenome.git
 cd nlp2phenome
 
 # unzip pretrained models
 cd "${install_dir}"/semehr/nlp2phenome/pretrained_models
 unzip stroke_settings.zip && unzip stroke_subtype_models.zip && unzip stroke_supplemental-gazetteer.zip
 # copy supplemental gaz to bio-yodie folder
 cp supplemental-gazetteer "${install_dir}"/gcp/bio-yodie-1-2-1/finalize -R -f
 # replace placeholders with installation paths
 sed -i -e "s@PH_INSTALLATION_PATH@$install_dir@g" ./stroke_settings/prediction_task.json
 sed -i -e "s@PH_INSTALLATION_PATH@$install_dir@g" ./stroke_settings/doc_infer.json
 cd "${install_dir}"/semehr/nlp2phenome
else
 echo 'nlp2phenome reop exists'
 cd nlp2phenome
 git pull
fi
pip3 install -r requirements.txt
python3 -m spacy download en_core_web_sm

echo "installation finished at ${install_dir} successfully."

