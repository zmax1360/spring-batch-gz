# spring-batch-gz

Minimal Spring Batch (Boot 3.3 + Batch 5) project that:
- Reads `.gz` files from a directory (each contains a `.txt`)
- Only processes files whose **filename contains `nomatch`**
- Reads each line, parses fields (timestamp, serviceName, reactTime, destinationSeq, seq, entity, ...)
- Routes output by `serviceName` and logs to console
- Emits detailed logs for Job/Step/Chunk and item errors

## Run

```bash
mvn -q -DskipTests package
java -jar target/spring-batch-gz-0.0.1-SNAPSHOT.jar   --spring.batch.job.name=importJob   --app.inputDir=file:./data   --app.glob=**/*.gz   --app.filenameMustContain=nomatch   --app.chunkSize=500
```

Sample `.gz` is under `data/`. You should see lines printed with `[SERVICE=...]` prefixes and step/chunk summaries.
