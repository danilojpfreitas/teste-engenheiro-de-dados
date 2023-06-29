# Teste de Eng. de Dados de Danilo Freitas - Mesha Tecnologia

![Mesha](img/mesha.png)

Critérios avaliadas:
- Docker :white_check_mark:
- SQL :white_check_mark:
- Python :white_check_mark:
- Organização do Código :white_check_mark:
- Documentação :white_check_mark:
- ETL :white_check_mark:
- Modelagem dos dados :white_check_mark:

Desejáveis
- PySpark :white_check_mark:
- Esquema Estrela :white_check_mark:

### Base de Dados:

- A base de dados selecionada para esse desafio técnico está disponível em: [Base de Dados](https://download.inep.gov.br/microdados/microdados_enem_2020.zip).

- Após o download e da descompactação o arquivo csv utilizado está localizado em `microdados_enem_2020/DADOS/MICRODADOS_ENEM_2020.csv`.

### Perguntas respondidas:

1. Qual a escola com a maior média de notas? :white_check_mark:
2. Qual o aluno com a maior média de notas e o valor dessa média? :white_check_mark:
3. Qual a média geral? :white_check_mark:
4. Qual o % de Ausentes? :white_check_mark:
5. Qual o número total de Inscritos? :white_check_mark:
6. Qual a média por disciplina? :white_check_mark:
7. Qual a média por Sexo? :white_check_mark:
8. Qual a média por Etnia? :white_check_mark:

### Arquitetura e Features escolhidas para o projeto

![arquitetura](img/arquitetura.png)

Visando atender aos critérios deste projeto e da utilização das principais features utilizadas no mercado, as seguintes ferramentas foram utilizadas:

- Airflow: Processo ETL totalmente orquestrado e de fácil replicação;
- PySpark: Uma das principais bibliotecas de Big Date atual;
- Parquet: Formato de arquivo mais performatico;
- MySQL: Requisito do projeto é um dos DBs mais utilizados no mercado;
- Jupyter: Notebook de desenvolvimento do projeto.

### Step-by-Step

## 1) Desenvolvimento do código PySpark

O primeiro passo do projeto foi o desenvolvimento do código PySpark. Para isso, após o download e a descompactação do arquivo foi criado o diretório dados e dentro dele o diretório raw. Dentro do diretório row foi disponibilizado o arquivo csv: `MICRODADOS_ENEM_2020.csv`.

Um segundo diretório foi criado de nome notebook e dentro dele o arquivo `desenvolvimento.ipynb`. O seu acesso foi por meio do Jupyter, em:

  ```  jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root ```

Dentro do notebook `desenvolvimento.ipynb` foi desenvolvido todo o código que posteriormente irá ser apresentado.

![jupyter](img/jupyter.png)