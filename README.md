# Big-Data-Project-2
Customer Data Analysis with Spark


## Setting up your Python env using uv...

See instructions for setting up uv here: 

https://docs.astral.sh/uv/getting-started/installation/

Or if you're on Linux / macOS, simply run

```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

And add `~/.cargo/bin` to your `$PATH`.

Then `cd` in the root of this repo and run

```
uv sync
```

This will create an env in `.venv`, which you can activate with

```
source .venv/bin/activate
```

## Data Ingestion 

Make sure the file  is available in the `/data/BigData/your_file.csv` directory on the host machine, as this will be the input for ingestion.

### Start the HDFS cluster

Run the command the start the HDFS cluster 

```
docker-compose up -d
```
You can access the Hadoop web UI via the browser:

NameNode Web UI: http://localhost:9870
DataNode Web UI: http://localhost:9864

### Start the script 

```
python data_ingestion/data_ingestion.py
```

