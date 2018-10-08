%%bash

OUT_DIR="assignment1_"$(date +"%s%6N")

NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming wordCount" \
    -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D stream.map.output.field.separator='\t' \
    -D stream.num.map.output.key.fields=4 \
    -D map.output.key.field.separator=. \
    -D mapred.text.key.comparator.options=-k2,2nr \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper1.py" \
    -combiner "python reducer1.py" \
    -reducer "python reducer1.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null

# Code for obtaining the results
hdfs dfs -cat ${OUT_DIR}/part-00000 | sed -n '7p;8q'
hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null
