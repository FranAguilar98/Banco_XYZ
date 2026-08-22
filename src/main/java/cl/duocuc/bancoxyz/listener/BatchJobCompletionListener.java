package cl.duocuc.bancoxyz.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BatchJobCompletionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("==================================================================");
        log.info(">>> INICIANDO JOB: {}", jobExecution.getJobInstance().getJobName());
        log.info("==================================================================");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("------------------------------------------------------------------");
        log.info(">>> JOB '{}' finalizado con estado: {}",
                jobExecution.getJobInstance().getJobName(), jobExecution.getStatus());

        for (StepExecution step : jobExecution.getStepExecutions()) {
            log.info("    Step [{}] -> leidos: {} | escritos: {} | saltados (skip): {} | estado: {}",
                    step.getStepName(),
                    step.getReadCount(),
                    step.getWriteCount(),
                    step.getSkipCount(),
                    step.getStatus());
        }

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            jobExecution.getAllFailureExceptions()
                    .forEach(e -> log.error("    Excepcion registrada: {}", e.getMessage(), e));
        }
        log.info("==================================================================");
    }
}
