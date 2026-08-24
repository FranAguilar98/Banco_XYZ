package cl.duocuc.bancoxyz.policy;
 
import cl.duocuc.bancoxyz.exception.DatoInvalidoException;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
 
import java.sql.SQLException;
 
public class GenericSkipPolicy implements SkipPolicy {
 
    private final int skipLimit;
 
    public GenericSkipPolicy(int skipLimit) {
        this.skipLimit = skipLimit;
    }
 
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        if (t instanceof SQLException) {
            return false;
        }
        if (t instanceof DatoInvalidoException || t instanceof FlatFileParseException) {
            if (skipCount >= skipLimit) {
                throw new SkipLimitExceededException(skipLimit, t);
            }
            return true;
        }
        if (skipCount >= skipLimit) {
            throw new SkipLimitExceededException(skipLimit, t);
        }
        return true;
    }
}