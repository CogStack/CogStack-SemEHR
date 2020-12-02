## installation of minimised CogStack-SemEHR and nlp2phenome
**NB: no need to clone repos by yourself, the script will checkout the most relevant branches**

1. download the two files into your computer
   ```bash
   # bash script for installation
   https://github.com/CogStack/CogStack-SemEHR/blob/safehaven_mini/installation/install_semehr.sh
   
   # semehr configuration template
   https://github.com/CogStack/CogStack-SemEHR/blob/safehaven_mini/installation/semehr_conf_template.json
   ```

2. run the downloaded bash script
   ```bash
   sh install_semehr.sh
   ```
   It will ask for a full path to install relevant software. All packages and repos will be installed there.

3. folder structure
   When it is installed successfully, the installation folder will contain the following folders.
   ```
   gcp - contains Java based NLP core software packages
   semehr - contains two Github repos of NLP and machine learning modules
   data - the working folder, which contains the following subfolders
        - input_docs: the free text documents to be analysed
        - output_docs: the NLP raw outputs
        - semehr_results: the semehr post processed results
        - phenome_results: the text phenotyping results
   ```
