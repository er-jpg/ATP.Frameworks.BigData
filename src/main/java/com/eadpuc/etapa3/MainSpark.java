/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.eadpuc.etapa3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author bruno.saragosa
 */
public class MainSpark {
    public static void main(String [] args){       
        // Configuração do spark para usar o master como localhost sem limite de threads
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Etapa3");
        
        // Usando o spark session
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .appName("Teste")
                .getOrCreate();
        
        // Configuração para esconder logs
        //spark.sparkContext().setLogLevel("ERROR");
        
        // Cria um esquema, como o arquivo CSV, usado para mapear para dentro de uma tabela ao contrário de ficar usando arrays
        StructType schema = new StructType(new StructField[] {
                new StructField("dia", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("mes", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ano", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("bloco", DataTypes.StringType, false, Metadata.empty()),
                new StructField("tipo", DataTypes.StringType, false, Metadata.empty()),
                new StructField("descricao", DataTypes.StringType, false, Metadata.empty()),
                new StructField("descricaoLocalizacao", DataTypes.StringType, false, Metadata.empty()),
                new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                });      
        
        // Dataset com todos os crimes cometidos
        Dataset<Row> df = spark.read()
                // Define o delimiter como padrão do arquivo
                .option("delimiter", ";")
                // Avisa que não possui headers o arquivo
                .option("header", "false")
                // Esquema criado acima a ser mapeado
                .schema(schema)
                // Lê o arquivo propriamente dito
                .csv("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        
        // Cria uma view para serem realizadas as consultas sql
        df.createOrReplaceTempView("ocorrencias");
        
        // Epata 3
        // Quantidade de crimes por ano
        Dataset<Row> sqlCrimesPorAno = spark.sql("SELECT "
                + "ano, COUNT(*) as ocorrencias "
                + "FROM ocorrencias "
                + "GROUP BY ano "
                + "ORDER BY ano");
        
        
        // Quantidade de crimes por ano do tipo NARCOTICS
        Dataset<Row> sqlCrimesPorAnoNarc = spark.sql("SELECT "
                + "ano, COUNT(*) as ocorrencias "
                + "FROM ocorrencias "
                + "WHERE tipo = 'NARCOTICS'"
                + "GROUP BY ano "
                + "ORDER BY ano");
        
        
        // Quantidade de crimes por ano do tipo NARCOTICS e em dias pares
        Dataset<Row> sqlCrimesPorAnoNarcDiaPar = spark.sql("SELECT "
                + "ano, COUNT(*) as ocorrencias "
                + "FROM ocorrencias "
                + "WHERE tipo = 'NARCOTICS'"
                + "AND (dia % 2) == 0 "
                + "GROUP BY ano "
                + "ORDER BY ano");
        
        
        // Mês com a maior ocorrência de crimes
        Dataset<Row> sqlMesComMaiorOcorrencia = spark.sql("SELECT mes, a.ocorrencias "
                + "FROM (SELECT mes, "
                + "     COUNT(mes) as ocorrencias "
                + "     FROM ocorrencias "
                + "     GROUP BY mes "
                + "     ORDER BY 'ocorrencias' DESC) a LIMIT 1");
        
        
        // Mês com a maior média de ocorrência
        Dataset<Row> sqlMesComMaiorMediaOcorrencia = spark.sql("SELECT a.mes, AVG(a.count) AS ocorrencias "
                + "FROM ( SELECT COUNT(*) AS count, mes FROM ocorrencias GROUP BY mes ORDER BY count DESC) AS a "
                + "GROUP BY a.mes "
                + "ORDER BY ocorrencias DESC "
                + "LIMIT 1");
        
        
        // Mês por ano com a maior o ocorrência
        Dataset<Row> sqlMesAnoComMaiorOcorrencia = spark.sql("SELECT mes, ano, a.ocorrencias "
                + "FROM (SELECT mes, "
                + "     COUNT(mes) as ocorrencias, "
                + "     ano "
                + "     FROM ocorrencias "
                + "     GROUP BY mes, ano "
                + "     ORDER BY 'ocorrencias' DESC) a LIMIT 1");
        
        
        // Mês com a maior ocorrência do tipo DECEPTIVE PRACTICE
        Dataset<Row> sqlMesComMaiorOcorrenciaDecPrac = spark.sql("SELECT mes, a.ocorrencias "
                + "FROM (SELECT mes, "
                + "     COUNT(mes) as ocorrencias "
                + "     FROM ocorrencias "
                + "     WHERE tipo = 'DECEPTIVE PRACTICE' "
                + "     GROUP BY mes "
                + "     ORDER BY 'ocorrencias' DESC) a LIMIT 1");
        
        
        // Dia do ano com a maior ocorrência de crimes
        Dataset<Row> sqlDiaMesAnoComMaiorOcorrencia = spark.sql("SELECT ano, mes, dia, a.ocorrencias "
                + "FROM (SELECT mes, "
                + "     COUNT(mes) as ocorrencias, "
                + "     ano,"
                + "     dia "
                + "     FROM ocorrencias "
                + "     GROUP BY mes, ano, dia "
                + "     ORDER BY 'ocorrencias' DESC) a LIMIT 1");
        
        sqlCrimesPorAno.show();
        sqlCrimesPorAnoNarc.show();
        sqlCrimesPorAnoNarcDiaPar.show();
        sqlMesComMaiorOcorrencia.show();
        sqlMesComMaiorMediaOcorrencia.show();
        sqlMesAnoComMaiorOcorrencia.show();
        sqlMesComMaiorOcorrenciaDecPrac.show();
        sqlDiaMesAnoComMaiorOcorrencia.show();
    } 
}
