package cl.duocuc.bancoxyz.config;
 
import cl.duocuc.bancoxyz.listener.BatchJobCompletionListener;
import cl.duocuc.bancoxyz.listener.GenericSkipListener;
import cl.duocuc.bancoxyz.listener.GenericStepListener;
import cl.duocuc.bancoxyz.model.CuentaInteres;
import cl.duocuc.bancoxyz.model.CuentaInteresCsv;
import cl.duocuc.bancoxyz.policy.ChunkCompletionPolicy;
import cl.duocuc.bancoxyz.policy.GenericRetryPolicy;
import cl.duocuc.bancoxyz.policy.GenericSkipPolicy;
import cl.duocuc.bancoxyz.processor.CuentaInteresItemProcessor;
import cl.duocuc.bancoxyz.repository.CuentaInteresAnomaliaRepository;
import cl.duocuc.bancoxyz.repository.CuentaInteresRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
 
import java.math.BigDecimal;
 
 
@Configuration
@RequiredArgsConstructor
@Slf4j
public class JobInteresesMensualesConfig {
 
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final CuentaInteresItemProcessor cuentaInteresItemProcessor;
    private final CuentaInteresRepository cuentaInteresRepository;
    private final CuentaInteresAnomaliaRepository cuentaInteresAnomaliaRepository;
    private final GenericSkipListener genericSkipListener;
    private final BatchJobCompletionListener batchJobCompletionListener;
    private final TaskExecutor batchTaskExecutor;
 
    @Value("${app.batch.input.intereses}")
    private Resource interesesResource;
 
    @Value("${app.batch.chunk-size:5}")
    private int chunkSize;
 
    @Value("${app.batch.skip-limit:20}")
    private int skipLimit;
 
    private static final long CHUNK_MAX_DURATION_MS = 2000;
    private static final int RETRY_LIMIT = 3;
 
    @Bean
    public FlatFileItemReader<CuentaInteresCsv> cuentaInteresItemReader() {
        return new FlatFileItemReaderBuilder<CuentaInteresCsv>()
                .name("cuentaInteresItemReader")
                .resource(interesesResource)
                .linesToSkip(1) // encabezado: cuenta_id,nombre,saldo,edad,tipo
                .delimited()
                .names("cuentaId", "nombre", "saldo", "edad", "tipo")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(CuentaInteresCsv.class);
                }})
                .build();
    }
 
    @Bean
    public SynchronizedItemStreamReader<CuentaInteresCsv> cuentaInteresItemReaderSincronizado() {
        SynchronizedItemStreamReader<CuentaInteresCsv> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(cuentaInteresItemReader());
        return reader;
    }
 
    @Bean
    public ItemWriter<CuentaInteres> cuentaInteresItemWriter() {
        return chunk -> cuentaInteresRepository.saveAll(chunk.getItems());
    }
 
    @Bean
    public Step stepCalculoIntereses() {
        return new StepBuilder("stepCalculoIntereses", jobRepository)
                .<CuentaInteresCsv, CuentaInteres>chunk(
                        new ChunkCompletionPolicy(chunkSize, CHUNK_MAX_DURATION_MS),
                        transactionManager)
                .reader(cuentaInteresItemReaderSincronizado())
                .processor(cuentaInteresItemProcessor)
                .writer(cuentaInteresItemWriter())
                .taskExecutor(batchTaskExecutor)
                .faultTolerant()
                .skipPolicy(new GenericSkipPolicy(skipLimit))
                .retryPolicy(new GenericRetryPolicy(RETRY_LIMIT))
                .listener(genericSkipListener)
                .listener(new GenericStepListener())
                .build();
    }
 
    @Bean
    public Tasklet resumenInteresesTasklet() {
        return (contribution, chunkContext) -> {
            var cuentas = cuentaInteresRepository.findAll();
            BigDecimal totalInteresGenerado = cuentas.stream()
                    .map(CuentaInteres::getInteresGenerado)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            long totalAnomalias = cuentaInteresAnomaliaRepository.count();
 
            log.info("Calculo de intereses finalizado. Cuentas procesadas: {}. Interes total generado: {}. Anomalias: {}",
                    cuentas.size(), totalInteresGenerado, totalAnomalias);
 
            return org.springframework.batch.repeat.RepeatStatus.FINISHED;
        };
    }
 
    @Bean
    public Step stepResumenIntereses() {
        return new StepBuilder("stepResumenIntereses", jobRepository)
                .tasklet(resumenInteresesTasklet(), transactionManager)
                .build();
    }
 
    @Bean
    public Job calculoInteresesMensualesJob() {
        return new JobBuilder("calculoInteresesMensualesJob", jobRepository)
                .listener(batchJobCompletionListener)
                .start(stepCalculoIntereses())
                .next(stepResumenIntereses())
                .build();
    }
}
 