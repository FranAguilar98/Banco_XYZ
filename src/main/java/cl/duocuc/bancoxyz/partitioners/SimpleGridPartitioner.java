package cl.duocuc.bancoxyz.partitioners;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Partitioner generico y reutilizable para los 3 Jobs.
 * Crea "gridSize" particiones (una por hilo). Cada particion recibe en su
 * ExecutionContext su indice (partitionIndex) y el total de particiones
 * (gridSize). El ItemProcessor de cada Job usa esos dos valores para decidir
 * si un registro le corresponde a esta particion (mediante modulo), evitando
 * que dos particiones procesen o dupliquen el mismo registro.
 */
@Component
public class SimpleGridPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> particiones = new HashMap<>();
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext contexto = new ExecutionContext();
            contexto.putInt("partitionIndex", i);
            contexto.putInt("gridSize", gridSize);
            particiones.put("partition" + i, contexto);
        }
        return particiones;
    }
}