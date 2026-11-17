package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Незмінна модель транзакції в електронній комерції.
 *
 * @param transactionId ідентифікатор транзакції
 * @param category      категорія товару
 * @param amount        сума транзакції
 * @param timestamp     час проведення транзакції
 */
public record Transaction(String transactionId, String category,
                          BigDecimal amount, LocalDateTime timestamp) {
}
