# ATP Frameworks Big Data

Esse repositório contempla todo o código fonte para a solução da ATP proposta para a matéria de Frameworks de Big Data

Além de possuir o log de execução com todas as consultas, seguindo a ordem proposta na Etapa 3 da atividade.

## Instruções

### Requerimentos

- NetBeans IDE 8.2 [Link aqui](https://netbeans.org/downloads/old/8.2/)
- Apache Maven 3.6.3 [Link aqui](https://maven.apache.org/download.cgi)
- Apache Hadoop [Link aqui](https://hadoop.apache.org/releases.html)

### Configurações
O primeiro ponto é estar certo de que o projeto está usando os pacotes corretamente do maven, que são
- `org.apache.spark`
	1. `spark-core_2.12`, `3.0.0`
	2. `spark-sql_2.12`, `3.0.0`

O segundo passo é ter o Dataset requerido no ambiente do Hadoop, que pode ser alterado na classe `MainSpark`
dentro da leitura do arquivo, está comentado `Lê o arquivo propriamente dito`.

Não existe exportação de resultados, tudo deve ser lido direto do log de execução da aplicação, por isso, é recomendado
que salve o arquivo após a execução para extrair os dados, existe uma cópia da ultima execução dentro desse repositório
com o nome de `spark.log` para consulta.