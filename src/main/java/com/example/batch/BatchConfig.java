package com.example.batch;

import com.example.batch.io.FilteredResources;
import com.example.batch.io.GzipLineItemReader;
import com.example.batch.log.LoggingListeners;
import com.example.batch.log.LoggingListeners.ChunkLog;
import com.example.batch.log.LoggingListeners.ReadLog;
import com.example.batch.log.LoggingListeners.ItemLog;
import com.example.batch.log.LoggingListeners.JobLog;
import com.example.batch.log.LoggingListeners.StepLog;
import com.example.batch.processor.DefaultProcessor;
import com.example.batch.processor.PricingProcessor;
import com.example.batch.processor.RiskProcessor;
import com.example.batch.processor.SettlementProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.support.ClassifierCompositeItemProcessor;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;
import java.util.List;

@Configuration
public class BatchConfig {
  private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

  @Value("${app.inputDir}") String inputDir;
  @Value("${app.glob}") String glob;
  @Value("${app.filenameMustContain}") String mustContain;
  @Value("${app.chunkSize:500}") int chunkSize;

  @Bean
  public MultiResourceItemReader<String> reader() {
    Resource[] resources = FilteredResources.gzWithName(inputDir, glob, mustContain);
    if (resources.length == 0) {
      throw new IllegalStateException(
              "No input .gz files found. Check inputDir/glob/permissions. " +
                      "inputDir=" + inputDir + " glob=" + glob + " filenameMustContain=" + mustContain
      );
    }
    log.info("Resolved {} input files", resources.length);
    var mr = new MultiResourceItemReader<String>();
    mr.setResources(resources);
    var delegate = new GzipLineItemReader();
    mr.setDelegate(delegate); // MultiResource will set resource on the delegate
    return mr;
  }

  @Bean(name = "lineToRecordProcessor")
  public ItemProcessor<String, LogRecord> lineToRecordProcessor() {
    return line -> {
      if (line == null || line.isBlank()) return null;
      var fields = Arrays.stream(line.split(",", -1)).map(String::trim).toList();
      return new LogRecord(line, fields);
    };
  }
  @Bean(name = "perServiceProcessor")
  ItemProcessor<LogRecord, LogRecord> perServiceProcessor(
          @Qualifier("pricingProcessor")  PricingProcessor pricing,
          @Qualifier("settlementProcessor")  SettlementProcessor settlement,
          @Qualifier("riskProcessor")  RiskProcessor risk,
          @Qualifier("defaultProcessor")   DefaultProcessor fallback) {

    var router = new ClassifierCompositeItemProcessor<LogRecord, LogRecord>();
    router.setClassifier(record -> switch (record.serviceName().toLowerCase()) {
      case "pricing" -> pricing;
      case "settlement" -> settlement;
      case "risk" -> risk;
      default -> fallback;
    });
    return router;
  }
  @Bean public ItemWriter<LogRecord> writer() { return ConsoleWriters.byService(); }
  @Bean(name="processorPipeline")
  public ItemProcessor<String, LogRecord> processorPipeline(
          @Qualifier("lineToRecordProcessor") ItemProcessor<String, LogRecord> lineToRecordProcessor,
          ItemProcessor<LogRecord, LogRecord> perServiceProcessor) {

    return new CompositeItemProcessorBuilder<String, LogRecord>()
            .delegates(lineToRecordProcessor, perServiceProcessor)
            .build();
  }
  @Bean
  public Step importStep(JobRepository repo,
                         PlatformTransactionManager tx,
                         MultiResourceItemReader<String> reader,
                         @Qualifier("processorPipeline") ItemProcessor<String, LogRecord> processorPipeline,
                         ItemWriter<LogRecord> writer) {

    // modern executor config
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("io-");
    executor.setCorePoolSize(4);      // number of concurrent threads
    executor.setMaxPoolSize(4);
    executor.setQueueCapacity(0);     // force new threads rather than queueing
    executor.initialize();// optional, or control with throttleLimit

    return new StepBuilder("importStep", repo)
            .<String, LogRecord>chunk(chunkSize, tx)
            .reader(reader)
            .processor(processorPipeline)
            .writer(writer)
            .faultTolerant()
            .skipPolicy((t, count) -> true)
            .taskExecutor(executor)      // 👈 runs chunks in parallel
            .listener(new ReadLog())
            .build();
  }

  @Bean
  public Job importJob(JobRepository repo,  Step importStep) {
    return new JobBuilder("importJob", repo)
            .listener(new JobLog())
            .start(importStep)
            .build();
  }
  @Bean
  ApplicationRunner launchJob(JobLauncher launcher, Job importJob,
                              @Value("${app.inputDir}") String inputDir,
                              @Value("${app.glob}") String glob,
                              @Value("${app.filenameMustContain:}") String mustContain,
                              @Value("${app.chunkSize:500}") int chunkSize) {
    return args -> {
      var params = new JobParametersBuilder()
              .addString("inputDir", inputDir)
              .addString("glob", glob)
              .addString("mustContain", mustContain)
              .addString("chunkSize", String.valueOf(chunkSize))
              .addLong("run.id", System.currentTimeMillis()) // makes it unique
              .toJobParameters();

      launcher.run(importJob, params);
    };
  }
}
