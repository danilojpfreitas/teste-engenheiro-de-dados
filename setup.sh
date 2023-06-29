install() {

    echo "Instação do Container do Airflow..."
    docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password admin --firstname Danilo --lastname Freitas --role Admin --email admin@example.org); airflow webserver & airflow scheduler'
    
    echo "Acesso do Airflow UI em http://localhost:8080 para a geração da DAG."  

    echo "Conectando ao Container do Airflow..."
    docker container exec -it -u root airflow bash
    docker container exec -it airflow bash

    echo "Instalando as bibliotecas dentro do Container"
    docker container exec -it airflow bash
    pip install pymysql xlrd openpyxl pyspark findspark install-jdk

    docker container exec -it -u root airflow bash
    apt-get install wget
    apt-get install unzip
    apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

    echo "Instalando os arquivo Jars para conexão entre PySpark e MySQL"
    docker container exec -it -u root airflow bash
    mkdir jars
    cd jars
    wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
  
    echo "Instalando Raw dentro do Container"
    docker container exec -it airflow bash
    apt-get install wget
    mkdir dados
    cd dados
    mkdir raw
    mkdir processing
    cd raw
    wget --no-check-certificate https://download.inep.gov.br/microdados/microdados_enem_2020.zip
    unzip microdados_enem_2020.zip

}

jupyterdev() {

    echo "Acesso ao Notebook Jupyter para desenvolvimento do código PySpark..."
    jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root
    
}

data() {

    echo "Download dos dados"
    mkdir dados
    cd dados
    mkdir raw
    mkdir processing
    cd raw
    wget --no-check-certificate https://download.inep.gov.br/microdados/microdados_enem_2020.zip
    unzip microdados_enem_2020.zip
    mv DADOS/MICRODADOS_ENEM_2020.csv ..

}

conexaocontainer() {

    #Para conectar os containeres é necessario criar uma bridge de rede
    echo "1) Criar um network para conexão"
    docker network create my-net
    echo "2) Criar uma conexão da network para cada container"
    docker network connect my-net db
    docker network connect my-net airflow
    echo "3) Inspecionar o IPAddress do network my-net de db e implementar na DAG"

}