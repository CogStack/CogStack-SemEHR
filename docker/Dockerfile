#
# SemEHR Dockerfile
#
FROM ubuntu:latest
MAINTAINER Honghan Wu "honghan.wu@gmail.com"

########
# Pre-reqs
########
RUN apt-get update \
&& apt-get -y install software-properties-common
# && add-apt-repository ppa:webupd8team/java

RUN apt-get update
# RUN echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections

RUN apt-get install -y \
	ant \
	curl \
#	oracle-java8-installer \
	openjdk-11-jdk \
	subversion \
	unzip \
	vim \
	git

# ENV JAVA_HOME /usr/lib/jvm/java-8-oracle/
ENV JAVA_HOME /usr/lib/jvm/open-jdk/


########
# GCP, Gate, Bio-Yodie
########
RUN mkdir /opt/gcp
WORKDIR '/opt/gcp'

ENV JAVA_TOOL_OPTIONS '-Dfile.encoding=UTF8'

RUN cd /opt/gcp
#at this moment, bio-yodie requires this particular subversion of GCP
RUN svn co http://svn.code.sf.net/p/gate/code/gcp/trunk@18658 gcp-2.5-18658
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN cd /opt/gcp/gcp-2.5-18658 && ant
ENV GCP_HOME '/opt/gcp/gcp-2.5-18658'

RUN curl -L 'http://netix.dl.sourceforge.net/project/gate/gate/8.1/gate-8.1-build5169-ALL.zip' > gate-8.1-build5169-ALL.zip && unzip gate-8.1-build5169-ALL.zip && mv gate-8.1-build5169-ALL gate && rm gate-8.1-build5169-ALL.zip
ENV GATE_HOME '/opt/gcp/gate'

WORKDIR '/opt/gcp/'
ENV PATH "$PATH:$GCP_HOME:$GATE_HOME/bin"

RUN curl -L 'https://cogstack.rosalind.kcl.ac.uk/exports/bio-yodie-1.2.1-se.tar.gz' > bio-yodie-1.2.1-se.tar.gz && tar xzvf bio-yodie-1.2.1-se.tar.gz && rm bio-yodie-1.2.1-se.tar.gz
RUN mv bio-yodie-1.2.1 bio-yodie-1-2-1

RUN cd /opt/gcp/gcp-2.5-18658/lib
WORKDIR '/opt/gcp/gcp-2.5-18658/lib'
RUN curl -L 'https://cogstack.rosalind.kcl.ac.uk/exports/customised_handlers.tar.gz' > customised_handlers.tar.gz && tar xzvf customised_handlers.tar.gz && cp customised_handlers/* ./ && rm -fr customised_handlers && rm -f customised_handlers.tar.gz

########
# python & libraries for SemEHR
########
RUN apt-get update
RUN apt-get install -y \
    python \
    python-pip \
    wget \
    python-setuptools \
    python-dev \
    libxml2-dev \
    libxslt-dev \
    zlib1g-dev \
    build-essential

RUN pip install requests
RUN pip install lxml
RUN pip install pyquery
RUN pip install joblib
# RUN pip install hashlib
# RUN easy_install hashlib
RUN pip install urllib3
RUN pip install Elasticsearch
RUN apt-get -y install python-mysqldb
#RUN pip install MySQL-python

RUN apt-get -y install unixodbc unixodbc-dev libmysqlclient-dev freetds-dev tdsodbc
RUN pip install pyodbc

# mysql odbc
RUN curl -L 'https://cdn.mysql.com//Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit.tar.gz' > mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit.tar.gz && tar xzvf mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit.tar.gz && cp mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit/lib/* /usr/lib && mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit/bin/myodbc-installer -d -n MySQL -a -t DRIVER=/usr/lib/libmyodbc8w.so && rm -fr mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit && rm -f mysql-connector-odbc-8.0.16-linux-ubuntu19.04-x86-64bit.tar.gz

########
# SemEHR
########
RUN mkdir /opt/semehr
WORKDIR '/opt/semehr'
RUN cd /opt/semehr
# RUN git clone https://github.com/CogStack/CogStack-SemEHR.git
RUN mkdir /opt/semehr/CogStack-SemEHR


ENV semehr_path '/opt/semehr/CogStack-SemEHR'
ENV PATH "$PATH:$semehr_path:/opt/semehr/"
ENV CLASSPATH "$GATE_HOME/bin"

# RUN cp ./docker/semehr.sh ./
RUN curl -L 'https://cogstack.rosalind.kcl.ac.uk/exports/semehr.sh.txt' > semehr.sh
RUN chmod a+x semehr.sh

RUN mkdir /data/
RUN mkdir /data/output_docs
RUN mkdir /data/input_docs
RUN mkdir /data/smehr_results

########
# entrypoint
########
ENTRYPOINT ["semehr.sh"]
