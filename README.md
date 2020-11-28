# DFS-Helper
Deleting old hdfs files from a path. There are shell script alternatives but they keep
stopping/starting the jvm so are significantly slower.
Originally I used a simple recursive function but especially when doing multithreading it was easy to run out of memory
so I switched to a Stack. This made it lightning fast.

## How to build
It's a typical maven project creating a fat jar in /target
``
mvn clean install
``

## Prerequests
It currently expects conf files in these spots: /etc/hadoop/conf/core-site.xml /etc/hadoop/conf/hdfs-site.xml
Easy to make configurable but I didn't need it to be.

## Example on how to run
Not doing the actual delete and really verbose output:
``
java -jar dfs-helper-0.0.1-SNAPSHOT.jar --path=/user/bschut/ --olderthan=300 --dryrun --verbose
``
Speed it up a bit and do the actual delete for things older than 14 days:
```
java -jar dfs-helper-0.0.1-SNAPSHOT.jar --path=/user/bschut/ --olderthan=14 --threads=16
```

# Shell alternative
The original shell script I used. I wouldn't use it but figured I would share it as an alternative. It's horribly slow.
```
#!/bin/bash
usage="Usage: ./nameofthescript.sh [path] [days]"

if [ ! "$1" ]
then
  echo $usage;
  exit 1;
fi

if [ ! "$2" ]
then
  echo $usage;
  exit 1;
fi

now=$(date +%s);

# Loop through files
export HADOOP_CLIENT_OPTS=' -XX:-UseGCOverheadLimit -Xmx16G '
sudo -E -u hdfs hdfs dfs -ls $1 | while read f; do
  # Get File Date and File Name
  file_date=`echo $f | awk '{print $6}'`;
  file_name=`echo $f | awk '{print $8}'`;

  # Calculate Days Difference
  difference=$(( ($now - $(date -d "$file_date" +%s)) / (24 * 60 * 60) ));
  if [ $difference -gt $2 ]; then
    # Insert delete logic here
    echo "This file $file_name is dated $file_date.";
    export HADOOP_CLIENT_OPTS=' -XX:-UseGCOverheadLimit -Xmx200m -XX:TieredStopAtLevel=1 -XX:CICompilerCount=1 -XX:+UseSerialGC -XX:-UsePerfData'
    sudo -E -u hdfs hdfs dfs -rm -skipTrash -R $file_name &
    [ $( jobs | wc -l ) -ge 8 ] && wait
  fi
done
```