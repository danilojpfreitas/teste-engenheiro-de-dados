from mysqlairflow import url, driver, user, password
from datetime import datetime,date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType 
from pyspark.sql import functions as f
import findspark

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
}

dag = DAG('etl_data', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

findspark.init()

spark = SparkSession \
        .builder \
        .appName('PySpark MySQL Connection') \
        .master('local[*]') \
        .config("spark.jars", "/opt/airflow/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

pathRow = "/opt/airflow/dados/raw/DADOS/MICRODADOS_ENEM_2020.csv"
pathParquetProcessing = "/opt/airflow/dados/processing"
pathCurated = "/opt/airflow/dados/curated"

def extracao_dados():

    #Extração dos dados

    enem = spark.read.csv(pathRow, sep=";", header=True)

    enem.printSchema()
    
    #Seleção das Colunas

    enemSelecao = enem.select("NU_INSCRICAO", "TP_SEXO", "TP_COR_RACA", "CO_MUNICIPIO_ESC", "NO_MUNICIPIO_ESC", "SG_UF_ESC", "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO", "TP_ST_CONCLUSAO")

    #Correção dos tipos dos dados

    enemSelecaoTipos = enemSelecao\
    .withColumn(
    "TP_COR_RACA",
    enemSelecao["TP_COR_RACA"].cast(IntegerType())
    )\
    .withColumn(
    "CO_MUNICIPIO_ESC",
    enemSelecao["CO_MUNICIPIO_ESC"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_CN",
    enemSelecao["TP_PRESENCA_CN"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_CH",
    enemSelecao["TP_PRESENCA_CH"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_LC",
    enemSelecao["TP_PRESENCA_LC"].cast(IntegerType())
    )\
    .withColumn(
    "TP_PRESENCA_MT",
    enemSelecao["TP_PRESENCA_MT"].cast(IntegerType())
    )\
    .withColumn(
    "NU_NOTA_CN",
    enemSelecao["NU_NOTA_CN"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_CH",
    enemSelecao["NU_NOTA_CH"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_LC",
    enemSelecao["NU_NOTA_LC"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_MT",
    enemSelecao["NU_NOTA_MT"].cast(DoubleType())
    )\
    .withColumn(
    "NU_NOTA_REDACAO",
    enemSelecao["NU_NOTA_REDACAO"].cast(IntegerType())
    )\
    .withColumn(
    "TP_ST_CONCLUSAO",
    enemSelecao["TP_ST_CONCLUSAO"].cast(IntegerType())
    )

    novosNomes = {'NU_INSCRICAO':'NumInscricao', 'TP_SEXO':'Sexo', 'TP_COR_RACA':'Etnia', 'CO_MUNICIPIO_ESC':'CodMunicipioEsc', 'NO_MUNICIPIO_ESC':'NomMunicipioEsc', 'SG_UF_ESC':'EstadoEsc', 'TP_PRESENCA_CN':'PresencaCN', 'TP_PRESENCA_CH':'PresencaCH', 'TP_PRESENCA_LC':'PresencaLC', 'TP_PRESENCA_MT':'PresencaMT', 'NU_NOTA_CN':'NotaCN', 'NU_NOTA_CH':'NotaCH', 'NU_NOTA_LC':'NotaLC', 'NU_NOTA_MT':'NotaMT', 'NU_NOTA_REDACAO':'NotaRedacao', 'TP_ST_CONCLUSAO':'SituacaoConclusao'}

    for key, value in novosNomes.items():
        enemSelecaoTipos = enemSelecaoTipos.withColumnRenamed(key, value)

    #Transformação para o formato Parquet - Processing Zone

    enemSelecaoTipos.write.parquet(
    pathParquetProcessing,
    mode = "overwrite"
    )


def pergunta_01_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 1 - Qual a escola com a maior média de notas?

    escolaMaiorMedia = spark.sql("""
        SELECT 
            CodMunicipioEsc, NomMunicipioEsc, 
            EstadoEsc, ROUND(AVG(MediaNotas), 2) AS MediaNotas
        FROM
            (SELECT
                NumInscricao, CodMunicipioEsc, NomMunicipioEsc, EstadoEsc, 
                ROUND((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5, 2) AS MediaNotas
            FROM
                enemView
            WHERE 
                    SituacaoConclusao == 2 
                AND 
                    CodMunicipioEsc IS NOT NULL 
                AND 
                    NotaMT IS NOT NULL
                AND
                    NotaCH IS NOT NULL
                AND
                    NotaCN IS NOT NULL
                AND
                    NotaLC IS NOT NULL
                AND 
                    NotaRedacao IS NOT NULL
            )
        GROUP BY CodMunicipioEsc, NomMunicipioEsc, EstadoEsc  
        ORDER BY MediaNotas DESC
        """)

    escolaMaiorMedia = escolaMaiorMedia.limit(1)

    escolaMaiorMedia.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "01escolamaiormedia")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_02_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 2 - Qual o aluno com a maior média de notas e o valor dessa média?

    # Dataframe and View Etnia
    etniaColumns = ["codEtnia", "nomeEtnia"]
    etniaNome = [(0, "Nao declarado"), (1, "Branca"), (2, "Preta"), (3, "Parda"), (4, "Amarela"), (5, "Indigena")]
    etniaDataframe = spark.createDataFrame(etniaNome, etniaColumns)
    etniaDataframe.show()

    etniaDataframe.createOrReplaceTempView("etniaView")

    # Dataframe and View SituacaoConclusao
    sitConColumns = ["codCon", "nomeCon"]
    sitConNome = [(1, "Concluido"), (2, "Cursando/Conclusao 2020"), (3, "Cursando/Conclusao Apos 2020"), (4, "Sem Conclusao/Previsao")]
    sitConDataframe = spark.createDataFrame(sitConNome, sitConColumns)
    sitConDataframe.show()

    sitConDataframe.createOrReplaceTempView("sitConView")

    # Join Tables Etnia e SituaçãoConclusão
    alunoMaiorNota = spark.sql("""
        SELECT
            A.NumInscricao, A.Sexo,
            B.nomeEtnia AS Etnia,
            A.NomMunicipioEsc, A.EstadoEsc,
            C.nomeCon AS SituacaoConclusao,
            A.MediaNotas
        FROM
            (SELECT
                NumInscricao, Sexo, 
                Etnia, NomMunicipioEsc, 
                EstadoEsc, 
                SituacaoConclusao,
                ROUND((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5, 2) AS MediaNotas
            FROM
                enemView
            WHERE 
                    NotaMT IS NOT NULL
                AND
                    NotaCH IS NOT NULL
                AND
                    NotaCN IS NOT NULL
                AND
                    NotaLC IS NOT NULL
                AND 
                    NotaRedacao IS NOT NULL) A
        INNER JOIN
            etniaView B
            ON codEtnia == Etnia
        INNER JOIN
            sitConView C
            ON codCon == SituacaoConclusao
        ORDER BY MediaNotas DESC
        """)

    alunoMaiorNota = alunoMaiorNota.select("NumInscricao", "Sexo", "Etnia", "SituacaoConclusao", "MediaNotas").limit(1)

    alunoMaiorNota.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "02alunomaiornota")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_03_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 3 - Qual a média geral?

    mediaGeral = spark.sql("""
        SELECT 
            ROUND(AVG(ROUND((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5, 2)), 2) AS MediaGeral
        FROM
            (SELECT 
                *
            FROM
                enemView
            WHERE
                    NotaMT IS NOT NULL
                AND
                    NotaCH IS NOT NULL
                AND
                    NotaCN IS NOT NULL
                AND
                    NotaLC IS NOT NULL
                AND 
                    NotaRedacao IS NOT NULL
            )
        """)

    mediaGeral.limit(1).show()

    mediaGeral.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "03mediageral")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_04_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 4 - Qual o % de Ausentes?

    ausentesEnem = spark.sql("""
            SELECT
                COUNT(NumInscricao) AS NumAusentes
            FROM
                enemView
            WHERE 
                    PresencaCN = 0
                OR
                    PresencaCH = 0
                OR
                    PresencaLC = 0
                OR
                    PresencaMT = 0
        """)

    totalEnem = spark.sql("""
        SELECT
            COUNT(NumInscricao) AS NumTotal
        FROM
            enemView
        """)
    
    ausentesEnem.createOrReplaceTempView("ausentesEnemView")

    totalEnem.createOrReplaceTempView("totalEnemView")

    porcentagemAusentesEnem = spark.sql("""
        SELECT
            B.NumTotal, A.NumAusentes, 
            ROUND(((A.NumAusentes*100)/B.NumTotal), 2) AS PorcentagemAusentes
        FROM
            ausentesEnemView AS A,
            totalEnemView AS B
        """)

    porcentagemAusentesEnem.show()

    porcentagemAusentesEnem.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "04porceausentes")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_05_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 5 - Qual o número total de Inscritos?

    totalInscritos = spark.sql("""
        SELECT
            COUNT(NumInscricao) AS TotalInscritos
        FROM
            enemView 
        """)
    
    totalInscritos.show()

    totalInscritos.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "05totalinscritos")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_06_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 6 - Qual a média por disciplina?

    mediaDisciplinas = spark.sql("""
        SELECT
            A.MediaCN,
            B.MediaCH,
            C.MediaLC,
            D.MediaMT,
            E.MediaRedacao
        FROM
            (SELECT
                ROUND(AVG(NotaCN), 2) AS MediaCN
            FROM
                enemView
            WHERE
                NotaCN IS NOT NULL
            ) AS A,
            (SELECT
                ROUND(AVG(NotaCH), 2) AS MediaCH
            FROM
                enemView
            WHERE
                NotaCH IS NOT NULL
            ) AS B,
            (SELECT
                ROUND(AVG(NotaLC), 2) AS MediaLC
            FROM
                enemView
            WHERE
                NotaLC IS NOT NULL
            ) AS C,
            (SELECT
                ROUND(AVG(NotaMT), 2) AS MediaMT
            FROM
                enemView
            WHERE
                NotaMT IS NOT NULL
            ) AS D,
            (SELECT
                ROUND(AVG(NotaRedacao), 2) AS MediaRedacao
            FROM
                enemView
            WHERE
                NotaRedacao IS NOT NULL
            ) AS E
        """)

    mediaDisciplinas.show()

    mediaDisciplinas.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "06mediadisciplinas")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()
    

def pergunta_07_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 7 - Qual a média por Sexo?

    mediaSexo = spark.sql("""
        SELECT
            Sexo,
            ROUND(AVG(((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5)), 2) AS MediaNotaSexo
        FROM
            enemView
        WHERE 
                NotaMT IS NOT NULL
            AND
                NotaCH IS NOT NULL
            AND
                NotaCN IS NOT NULL
            AND
                NotaLC IS NOT NULL
            AND 
                NotaRedacao IS NOT NULL
        GROUP BY Sexo
        """)

    mediaSexo.show()

    mediaSexo.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "07mediasexo")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


def pergunta_08_curated_mysql():

    # Criação da View para as próximas DAGs
    enemParquet = spark.read.parquet(
    pathParquetProcessing
    )

    enemParquet.createOrReplaceTempView("enemView")

    # 8 - Qual a média por Etnia?

    # Dataframe and View Etnia

    etniaColumns = ["codEtnia", "nomeEtnia"]
    etniaNome = [(0, "Nao declarado"), (1, "Branca"), (2, "Preta"), (3, "Parda"), (4, "Amarela"), (5, "Indigena")]
    etniaDataframe = spark.createDataFrame(etniaNome, etniaColumns)
    etniaDataframe.show()

    etniaDataframe.createOrReplaceTempView("etniaView")

    #Selecao

    mediaEtnia = spark.sql("""
        SELECT
            B.nomeEtnia AS Etnia,
            A.MediaNotaEtnia
        FROM
            (SELECT
                Etnia,
                ROUND(AVG(((NotaCN+NotaCH+NotaLC+NotaMT+NotaRedacao)/5)), 2) AS MediaNotaEtnia
            FROM
                enemView
            WHERE 
                    NotaMT IS NOT NULL
                AND
                    NotaCH IS NOT NULL
                AND
                    NotaCN IS NOT NULL
                AND
                    NotaLC IS NOT NULL
                AND 
                    NotaRedacao IS NOT NULL
            GROUP BY Etnia) A
        INNER JOIN
            etniaView B
        ON codEtnia == Etnia
        """)

    mediaEtnia.show()

    mediaEtnia.write\
    .format("jdbc")\
    .option("driver",f"{driver}")\
    .option("url", f"jdbc:mysql://{url}")\
    .option("dbtable", "08mediaetnia")\
    .option("user", f"{user}")\
    .option("password", f"{password}")\
    .save()


extracao_dados = PythonOperator(
  task_id='extracao_dados',
  provide_context=True,
  python_callable=extracao_dados,
  dag=dag
)

pergunta_01_curated_mysql = PythonOperator(
  task_id='pergunta_01_curated_mysql',
  provide_context=True,
  python_callable=pergunta_01_curated_mysql,
  dag=dag
)

pergunta_02_curated_mysql = PythonOperator(
  task_id='pergunta_02_curated_mysql',
  provide_context=True,
  python_callable=pergunta_02_curated_mysql,
  dag=dag
)

pergunta_03_curated_mysql = PythonOperator(
  task_id='pergunta_03_curated_mysql',
  provide_context=True,
  python_callable=pergunta_03_curated_mysql,
  dag=dag
)

pergunta_04_curated_mysql = PythonOperator(
  task_id='pergunta_04_curated_mysql',
  provide_context=True,
  python_callable=pergunta_04_curated_mysql,
  dag=dag
)

pergunta_05_curated_mysql = PythonOperator(
  task_id='pergunta_05_curated_mysql',
  provide_context=True,
  python_callable=pergunta_05_curated_mysql,
  dag=dag
)

pergunta_06_curated_mysql = PythonOperator(
  task_id='pergunta_06_curated_mysql',
  provide_context=True,
  python_callable=pergunta_06_curated_mysql,
  dag=dag
)

pergunta_07_curated_mysql = PythonOperator(
  task_id='pergunta_07_curated_mysql',
  provide_context=True,
  python_callable=pergunta_07_curated_mysql,
  dag=dag
)

pergunta_08_curated_mysql = PythonOperator(
  task_id='pergunta_08_curated_mysql',
  provide_context=True,
  python_callable=pergunta_08_curated_mysql,
  dag=dag
)

extracao_dados >> [pergunta_01_curated_mysql, pergunta_02_curated_mysql, pergunta_03_curated_mysql, pergunta_04_curated_mysql, pergunta_05_curated_mysql, pergunta_06_curated_mysql, pergunta_07_curated_mysql, pergunta_08_curated_mysql]




    

