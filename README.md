# HiTest - HiPages Data Engineer Test

## Running
* To start a testing _Spark_ cluster with a master and worker container and a client to submit the job, run `docker-compose up --abort-on-container-exit`.
* By default, the job will process the data file in `./data/input/source_event_data.json`. To process a different file, provide the `SOURCE_PATH` (relative to `./data`) environment variable before running the `Docker Compose`.
* Output of the job will be written to `./data/output`, alternatively, you can provide `TARGET_DIR` (also relative to `./data`)

## Notes
### Data Integrity
Given the _JSON_ schema is provided, I am assuming all the data is correctly formatted and valid.
If I had more time available, I would add a _Spark_ schema generated from the _JSON_ schema and validate the data on loading.

### Streaming
It would be interesting to provide a streaming based solution as this kind of problem clearly leans itself to that.
Especially for the `hourly_activity` table it would be interesting to have live updates, but I am afraid I didn't have enough time for that.
