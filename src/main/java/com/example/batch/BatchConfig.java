package com.example.batch;

import com.example.batch.io.FilteredResources;
import com.example.batch.io.GzipLineItemReader;
import com.example.batch.log.LoggingListeners;
import com.example.batch.log.LoggingListeners.ChunkLog;
import com.example.batch.log.LoggingListeners.ReadLog;
import com.example.batch.log.LoggingListeners.ItemLog;
import com.example.batch.log.LoggingListeners.JobLog;
import com.example.batch.log.LoggingListeners.StepLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

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
  @Bean
  ItemReader<String> sanityReader() {
    return new org.springframework.batch.item.support.ListItemReader<>(List.of("a,b,c", "x,y,z"));
  }
  @Bean public ItemProcessor<String, LogRecord> processor() { return new LineToRecordProcessor(); }
  @Bean public ItemWriter<LogRecord> writer() { return ConsoleWriters.byService(); }

//  @Bean
//  public Step importStep(JobRepository repo, PlatformTransactionManager tx,
//                         MultiResourceItemReader<String> reader,
//                         ItemProcessor<String, LogRecord> processor,
//                         ItemWriter<LogRecord> writer) {
//    var itemLog = new ItemLog();
//    return new StepBuilder("importStep", repo)
//        .<String, LogRecord>chunk(chunkSize, tx)
//        .reader(reader)
//        .processor(processor)
//        .writer(writer)
//            .listener(new StepLog())
//            .listener(new ChunkLog())
//            .listener(new ReadLog())
//        .build();
//  }
  @Bean
  public Step importStep(JobRepository repo, PlatformTransactionManager tx,
                         @Qualifier("sanityReader") ItemReader<String> reader,   // 👈 swap here
                         ItemProcessor<String, LogRecord> processor,
                         ItemWriter<LogRecord> writer) {
    return new StepBuilder("importStep", repo)
            .<String, LogRecord>chunk(2, tx)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .listener(new ReadLog())  // optional: logs first few reads
            .build();
  }
  @Bean
  public Job importJob(JobRepository repo,  Step importStep) {
    return new JobBuilder("importJob", repo)
            .listener(new JobLog())
            .start(importStep)
            .build();
  }
}
