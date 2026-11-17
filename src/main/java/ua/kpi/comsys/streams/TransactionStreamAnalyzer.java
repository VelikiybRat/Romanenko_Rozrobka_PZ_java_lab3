package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

/**
 * Сервіс аналітики потоку транзакцій.
 *
 * <p>Усі методи безпечні для нескінченних стрімів: вони використовують лише ледачі
 * операції та операції короткого замикання ({@code findFirst}, {@code takeWhile})
 * або власний {@link Gatherer}, який сам зупиняє обробку.
 */
public class TransactionStreamAnalyzer {

    /**
     * Знаходить першу транзакцію, сума якої строго більша за {@code threshold}.
     * Транзакції з {@code amount == null} пропускаються.
     *
     * <p>На нескінченному стрімі метод завершується, щойно така транзакція знайдеться.
     * На скінченному стрімі без такої транзакції повертає {@link Optional#empty()}.
     *
     * @throws IllegalArgumentException якщо {@code stream} або {@code threshold} дорівнює {@code null}
     */
    public Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream,
                                                               BigDecimal threshold) {
        requireNonNull(stream, "stream");
        requireNonNull(threshold, "threshold");

        return stream
                .filter(Objects::nonNull)
                .filter(transaction -> Optional.ofNullable(transaction.amount())
                        .map(amount -> amount.compareTo(threshold) > 0)
                        .orElse(false))
                .findFirst();
    }

    /**
     * Бере транзакції з початку стріму, доки їхня накопичена сума не перевищує
     * {@code maxBudget}. Перша транзакція, з якою сума стала б більшою за бюджет,
     * у результат не потрапляє, і обробка на ній зупиняється.
     * Транзакція з {@code amount == null} вважається нульовою.
     *
     * <p>Зверніть увагу: якщо стрім нескінченний, а всі суми нульові, бюджет ніколи
     * не буде перевищено. Тому для нескінченних джерел суми мають бути додатними.
     *
     * @throws IllegalArgumentException якщо {@code stream} або {@code maxBudget} дорівнює {@code null},
     *                                  або {@code maxBudget} від'ємний
     */
    public List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream,
                                                                 BigDecimal maxBudget) {
        requireNonNull(stream, "stream");
        requireNonNull(maxBudget, "maxBudget");
        if (maxBudget.signum() < 0) {
            throw new IllegalArgumentException("maxBudget must not be negative");
        }

        BudgetTracker tracker = new BudgetTracker(maxBudget);
        return stream
                .sequential() // предикат зі станом коректний лише для послідовного стріму
                .filter(Objects::nonNull)
                .takeWhile(tracker::tryAdd)
                .toList();
    }

    /**
     * Розбиває (можливо, нескінченний) стрім на послідовні батчі по {@code batchSize}
     * елементів за допомогою власного {@link Gatherer} і зупиняє обробку одразу після
     * формування рівно {@code maxBatches} батчів.
     *
     * <p>Якщо скінченний стрім закінчився раніше, останній неповний батч також
     * повертається (як у {@code Gatherers.windowFixed}).
     *
     * @throws IllegalArgumentException якщо {@code stream == null}, {@code batchSize <= 0}
     *                                  або {@code maxBatches <= 0}
     */
    public List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches) {
        requireNonNull(stream, "stream");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (maxBatches <= 0) {
            throw new IllegalArgumentException("maxBatches must be greater than 0");
        }

        return stream
                .gather(limitedBatches(batchSize, maxBatches))
                .toList();
    }

    /**
     * Кастомний послідовний Gatherer: групує елементи у вікна фіксованого розміру
     * та обмежує кількість вікон.
     *
     * <p>Стан ({@link BatchState}) зберігає поточний незаповнений батч і кількість
     * уже випущених батчів. Integrator повертає {@code false}, коли сформовано
     * {@code maxBatches} батчів, — це сигнал короткого замикання, тому upstream
     * (навіть нескінченний) більше не опитується.
     */
    static <T> Gatherer<T, ?, List<T>> limitedBatches(int batchSize, int maxBatches) {
        return Gatherer.ofSequential(
                () -> new BatchState<T>(batchSize),
                Gatherer.Integrator.<BatchState<T>, T, List<T>>of((state, element, downstream) -> {
                    state.current.add(element);
                    if (state.current.size() < batchSize) {
                        return !downstream.isRejecting();
                    }
                    boolean downstreamWantsMore = downstream.push(state.flush());
                    return downstreamWantsMore && state.emitted < maxBatches;
                }),
                (state, downstream) -> {
                    if (!state.current.isEmpty() && state.emitted < maxBatches && !downstream.isRejecting()) {
                        downstream.push(state.flush());
                    }
                });
    }

    /** Змінний стан Gatherer-а; належить одному виконанню стріму. */
    private static final class BatchState<T> {
        private final int batchSize;
        private List<T> current;
        private int emitted;

        private BatchState(int batchSize) {
            this.batchSize = batchSize;
            this.current = new ArrayList<>(batchSize);
        }

        private List<T> flush() {
            List<T> batch = List.copyOf(current);
            current = new ArrayList<>(batchSize);
            emitted++;
            return batch;
        }
    }

    /** Накопичувач суми для {@code takeWhile}; використовується лише в одному послідовному стрімі. */
    private static final class BudgetTracker {
        private final BigDecimal maxBudget;
        private BigDecimal total = BigDecimal.ZERO;

        private BudgetTracker(BigDecimal maxBudget) {
            this.maxBudget = maxBudget;
        }

        private boolean tryAdd(Transaction transaction) {
            BigDecimal amount = Optional.ofNullable(transaction.amount()).orElse(BigDecimal.ZERO);
            BigDecimal newTotal = total.add(amount);
            if (newTotal.compareTo(maxBudget) > 0) {
                return false;
            }
            total = newTotal;
            return true;
        }
    }

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
}
