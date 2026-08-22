package cl.duocuc.bancoxyz.processor;
 
import cl.duocuc.bancoxyz.exception.DatoInvalidoException;
import cl.duocuc.bancoxyz.model.Transaccion;
import cl.duocuc.bancoxyz.model.TransaccionAnomalia;
import cl.duocuc.bancoxyz.model.TransaccionCsv;
import cl.duocuc.bancoxyz.repository.TransaccionAnomaliaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
 
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
 
 
@Component
@StepScope
@RequiredArgsConstructor
@Slf4j
public class TransaccionItemProcessor implements ItemProcessor<TransaccionCsv, Transaccion> {
 
    private static final DateTimeFormatter FORMATO_CORRECTO = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATO_LEGACY = DateTimeFormatter.ofPattern("yyyy/MM/dd");
 
    private final TransaccionAnomaliaRepository anomaliaRepository;
 
    @Value("${app.batch.chunk-size:5}")
    private int chunkSize;
 
    private final Set<String> clavesVistas = new HashSet<>();
 
    @Override
    public Transaccion process(TransaccionCsv item) {
        Long idOrigen = parseIdSeguro(item.getId());
 
        BigDecimal monto;
        try {
            monto = new BigDecimal(item.getMonto().trim());
        } catch (Exception e) {
            return rechazar(idOrigen, item, "Monto invalido o vacio: '" + item.getMonto() + "'");
        }
        if (monto.compareTo(BigDecimal.ZERO) <= 0) {
            return rechazar(idOrigen, item, "Monto invalido (<= 0): " + monto);
        }
 
        LocalDate fecha = parsearFecha(item.getFecha());
        if (fecha == null) {
            return rechazar(idOrigen, item, "Formato de fecha invalido: '" + item.getFecha() + "'");
        }
 
        String tipo = item.getTipo() == null ? "" : item.getTipo().trim().toLowerCase();
        if (!tipo.equals("debito") && !tipo.equals("credito")) {
            return rechazar(idOrigen, item, "Tipo de transaccion invalido: '" + item.getTipo() + "'");
        }
 
        String clave = fecha + "|" + monto + "|" + tipo;
        if (!clavesVistas.add(clave)) {
            return rechazar(idOrigen, item, "Registro duplicado (fecha+monto+tipo repetidos)");
        }
 
        return Transaccion.builder()
                .idOrigen(idOrigen)
                .fecha(fecha)
                .monto(monto)
                .tipo(tipo)
                .build();
    }
 
    private LocalDate parsearFecha(String valor) {
        if (valor == null || valor.isBlank()) {
            return null;
        }
        String v = valor.trim();
        try {
            return LocalDate.parse(v, FORMATO_CORRECTO);
        } catch (DateTimeParseException ex1) {
            try {
                return LocalDate.parse(v, FORMATO_LEGACY);
            } catch (DateTimeParseException ex2) {
                return null;
            }
        }
    }
 
    private Long parseIdSeguro(String valor) {
        try {
            return Long.parseLong(valor.trim());
        } catch (Exception e) {
            return null;
        }
    }
 
    private Transaccion rechazar(Long idOrigen, TransaccionCsv original, String motivo) {
        log.warn("Transaccion rechazada [idOrigen={}]: {}", idOrigen, motivo);
        anomaliaRepository.save(TransaccionAnomalia.builder()
                .idOrigen(idOrigen)
                .datoOriginal(original.toString())
                .motivo(motivo)
                .fechaDeteccion(LocalDateTime.now())
                .build());
        throw new DatoInvalidoException(motivo);
    }
}
 