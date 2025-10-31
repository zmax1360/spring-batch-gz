package com.example.batch.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.listener.ItemListenerSupport;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

public class LoggingListeners {

  public static class JobLog extends JobExecutionListenerSupport {
    private static final Logger log = LoggerFactory.getLogger(JobLog.class);
    @Override public void beforeJob(JobExecution job) { log.info("JOB START name={} params={}", job.getJobInstance().getJobName(), job.getJobParameters()); }
    @Override public void afterJob(JobExecution job)  { log.info("JOB END   status={} read={} write={} skip={}",
        job.getStatus(),
        job.getStepExecutions().stream().mapToLong(StepExecution::getReadCount).sum(),
        job.getStepExecutions().stream().mapToLong(StepExecution::getWriteCount).sum(),
        job.getStepExecutions().stream().mapToLong(StepExecution::getSkipCount).sum());
    }
  }

  public static class StepLog extends StepExecutionListenerSupport {
    private static final Logger log = LoggerFactory.getLogger(StepLog.class);
    @Override public void beforeStep(StepExecution step) { log.info("STEP START {}", step.getStepName()); }
    @Override public ExitStatus afterStep(StepExecution step) {
      log.info("STEP END {} read={} filtered={} write={} skip={}",
          step.getStepName(), step.getReadCount(), step.getFilterCount(),
          step.getWriteCount(), step.getSkipCount());
      return step.getExitStatus();
    }
  }

  public static class ChunkLog extends ChunkListenerSupport {
    private static final Logger log = LoggerFactory.getLogger(ChunkLog.class);
    @Override public void beforeChunk(ChunkContext context) { log.info("CHUNK START {}", context.getStepContext().getStepName()); }
    @Override public void afterChunk(ChunkContext context)  { log.info("CHUNK END   {}", context.getStepContext().getStepName()); }
    @Override public void afterChunkError(ChunkContext context) { log.warn("CHUNK ERROR {}", context.getStepContext().getStepName()); }
  }

  public static class ItemLog extends ItemListenerSupport<Object, Object> {
    private static final Logger log = LoggerFactory.getLogger(ItemLog.class);
    @Override public void onProcessError(Object item, Exception e) { log.warn("PROCESS ERROR item={} msg={}", item, e.getMessage()); }

  }
}
