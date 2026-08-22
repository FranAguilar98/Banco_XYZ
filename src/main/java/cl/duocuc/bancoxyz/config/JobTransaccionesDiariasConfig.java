package cl.duocuc.bancoxyz.config;
 
import cl.duocuc.bancoxyz.listener.BatchJobCompletionListener;
import cl.duocuc.bancoxyz.listener.GenericSkipListener;
import cl.duocuc.bancoxyz.listener.GenericStepListener;
import cl.duocuc.bancoxyz.model.ResumenTransaccionDiaria;
import cl.duocuc.bancoxyz.model.Transaccion;
import cl.duocuc.bancoxyz.model.TransaccionCsv;
import cl.duocuc.bancoxyz.policy.ChunkCompletionPolicy;
import cl.duocuc.bancoxyz.policy.GenericRetryPolicy;
import cl.duocuc.bancoxyz.policy.GenericSkipPolicy;
import cl.duocuc.bancoxyz.processor.TransaccionItemProcessor;
import cl.duocuc.bancoxyz.repository.ResumenTransaccionDiariaRepository;
import cl.duocuc.bancoxyz.repository.TransaccionAnomaliaRepository;
import cl.duocuc.bancoxyz.repository.TransaccionRepository;
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
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
 
 
@Configuration
@RequiredArgsConstructor
@Slf4j
public class JobTransaccionesDiariasConfig {
 
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TransaccionItemProcessor transaccionItemProcessor;
    private final TransaccionRepository transaccionRepository;
    private final TransaccionAnomaliaRepository transaccionAnomaliaRepository;
    private final ResumenTransaccionDiariaRepository resumenRepository;
    private final GenericSkipListener genericSkipListener;
    private final BatchJobCompletionListener batchJobCompletionListener;
    private final TaskExecutor batchTaskExecutor;
 
    @Value("${app.batch.input.transacciones}")
    private Resource transaccionesResource;
 
    @Value("${app.batch.chunk-size:5}")
    private int chunkSize;
 
    @Value("${app.batch.skip-limit:20}")
    private int skipLimit;
 
    private static final long CHUNK_MAX_DURATION_MS = 2000;
    private static final int RETRY_LIMIT = 3;
 
 
    @Bean
    public FlatFileItemReader<TransaccionCsv> transaccionItemReader() {
        return new FlatFileItemReaderBuilder<TransaccionCsv>()
                .name("transaccionItemReader")
                .resource(transaccionesResource)
                .linesToSkip(1) // encabezado: id,fecha,monto,tipo
                .delimited()
                .names("id", "fecha", "monto", "tipo")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(TransaccionCsv.class);
                }})
                .build();
    }
 
    @Bean
    public SynchronizedItemStreamReader<TransaccionCsv> transaccionItemReaderSincronizado() {
        SynchronizedItemStreamReader<TransaccionCsv> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(transaccionItemReader());
        return reader;
    }
 
    @Bean
    public ItemWriter<Transaccion> transaccionItemWriter() {
        return chunk -> transaccionRepository.saveAll(chunk.getItems());
    }
 
    @Bean
    public Step stepCargaTransacciones() {
        return new StepBuilder("stepCargaTransacciones", jobRepository)
                .<TransaccionCsv, Transaccion>chunk(
                        new ChunkCompletionPolicy(chunkSize, CHUNK_MAX_DURATION_MS),
                        transactionManager)
                .reader(transaccionItemReaderSincronizado())
                .processor(transaccionItemProcessor)
                .writer(transaccionItemWriter())
                .taskExecutor(batchTaskExecutor)
                .faultTolerant()
                .skipPolicy(new GenericSkipPolicy(skipLimit))
                .retryPolicy(new GenericRetryPolicy(RETRY_LIMIT))
                .listener(genericSkipListener)
                .listener(new GenericStepListener())
                .build();
    }
 
 
    @Bean
    public Tasklet resumenDiarioTasklet() {
        return (contribution, chunkContext) -> {
            List<Transaccion> todas = transaccionRepository.findAll();
 
            Map<LocalDate, List<Transaccion>> porFecha = todas.stream()
                    .collect(Collectors.groupingBy(Transaccion::getFecha));
 
            long totalAnomalias = transaccionAnomaliaRepository.count();
 
            porFecha.forEach((fecha, lista) -> {
                BigDecimal totalDebito = lista.stream()
                        .filter(t -> "debito".equals(t.getTipo()))
                        .map(Transaccion::getMonto)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
 
                BigDecimal totalCredito = lista.stream()
                        .filter(t -> "credito".equals(t.getTipo()))
                        .map(Transaccion::getMonto)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
 
                resumenRepository.save(ResumenTransaccionDiaria.builder()
                        .fecha(fecha)
                        .totalDebito(totalDebito)
                        .totalCredito(totalCredito)
                        .cantidadTransacciones(lista.size())
                        .cantidadAnomalias((int) totalAnomalias)
                        .build());
            });
 
            log.info("Resumen diario generado para {} fechas. Anomalias totales detectadas: {}",
                    porFecha.size(), totalAnomalias);
 
            return org.springframework.batch.repeat.RepeatStatus.FINISHED;
        };
    }
 
    @Bean
    public Step stepResumenDiario() {
        return new StepBuilder("stepResumenDiario", jobRepository)
                .tasklet(resumenDiarioTasklet(), transactionManager)
                .build();
    }
 
 
    @Bean
    public Job reporteTransaccionesDiariasJob() {
        return new JobBuilder("reporteTransaccionesDiariasJob", jobRepository)
                .listener(batchJobCompletionListener)
                .start(stepCargaTransacciones())
                .next(stepResumenDiario())
                .build();
    }
}
 