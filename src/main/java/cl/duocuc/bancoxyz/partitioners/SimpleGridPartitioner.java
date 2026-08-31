package cl.duocuc.bancoxyz.partitioners;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

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