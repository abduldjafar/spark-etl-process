export env_for_run=$1

echo "buil scala jar"
current_date_time="`date +%Y%m%d%H%M%S`";
sbt package

function run_on_aws(){
    echo "upload jar to s3"
    aws --profile=qs s3 cp /path/to/file.jar   s3://path/scripts/

    echo "submit job to emr serverless"
    aws --profile=qs emr-serverless start-job-run \
        --application-id 00f6fp7l9eullo09 \
        --execution-role-arn arn:aws:iam::xxxxxxxx:role/emrRunTimeRoles \
        --name job_run_at_$current_date_time\
        --job-driver '{
            "sparkSubmit": {
                "entryPoint": " s3://path/scripts/sparketl_2.12-0.1.jar",
                "entryPointArguments":["https://hooks.slack.com/services/xxxxx/xxxxxx/xxxxxx","false","false"],
                "sparkSubmitParameters": "--packages com.lihaoyi:upickle_2.12:3.0.0-M1,com.amazon.deequ:deequ:2.0.1-spark-3.2,com.lihaoyi:requests_2.12:0.8.0 --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        }'

}

function run_on_local(){
    export AWS_ACCESS_KEY_ID=xxxxxxxxxxxx
    export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxx
    export AWS_PROFILE=yy
    spark-submit --packages com.github.housepower:clickhouse-spark-runtime-3.3_2.12:0.5.0,org.scala-lang:scala-reflect:2.12.10,io.delta:delta-core_2.12:2.0.0,org.apache.hadoop:hadoop-aws:3.2.4,com.typesafe:config:1.3.3,com.amazon.deequ:deequ:2.0.1-spark-3.2,software.amazon.awssdk:sso:2.19.17  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --class app.mflix.ProcessMflixTbGenres target/scala-2.12/sparketl_2.12-0.1.jar  https://hooks.slack.com/services/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 true true
}


if [[ $env_for_run -eq "local" ]]
then
  run_on_local
elif [[ $env_for_run -eq "aws" ]]
then
  run_on_aws
else
  echo "The variable is less than 10."
fi


