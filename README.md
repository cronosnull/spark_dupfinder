# spark_dupfinder
A spark process to find duplicate files. It is designed to scale, it requires a list of files, it can be created with the `createFileLists.sh` script. 
To use it in cluster mode the files and the environment need to be in a shared location (mounted in the same path in all nodes). 

# Local mode install
*Note*, in local mode it will be slower than other native solutions (this script have been desinged to scale into multiple nodes). You should consider tools like [fslint](http://www.pixelbeat.org/fslint/) or [fdupes](https://github.com/adrianlopezroche/fdupes) in this case.

  - Create the environment

 ```bash
 virtualenv -p python3 venv
 source venv/bin/activate
 pip install -r requirements.txt
```
 - Create the files list(s)
```bash
createFileLists.sh /mnt/c /mnt/e
```

  - This will create a file for each root path. Then you can run the python script, in local mode, with the glob expression as parameter. 
```bash
python spark_find_duplicates.py filelist*.txt.gz
```

# Cluster Mode (Spark standalone cluster)

  - Download and unpack Apache Spark in all the nodes, the current version was tested with Spark 3.0.2. 
  - Make sure java is installed and JAVA_HOME is set.
  - Start the master (using `<spark folder>/sbin/start-master -h 0.0.0.0`) in one of the nodes
  - Start the workers in all the nodes ( `<spark-folder>/sbin/start-slave.sh -h <host ip to bind> <master ip>:<port>`)
  - Set the PYSPARK_PYTHON environment variable to the python used in the virtualenv (that should be located on a shared location)
  - Use spark submit to run the application `<spark-folder>/bin/spark-submit  --master <master ip>:<port> spark_find_duplicates.py "<full patht to filelist*.txt.gz>"
  
# Cluster Mode (Yarn) 

If you have access to a YARN cluster, you can use the `--archives` option on spark submit to replicate the environment on all the nodes. You can create the environment package using venv-pack. 

# Options

```bash
spark_find_duplicates.py [OPTIONS] INPUT_FOLDER

Options:
  -o, --output_file TEXT
  -s, --min_size INTEGER  Min file size in bytes
  --help                  Show this message and exit.
```
