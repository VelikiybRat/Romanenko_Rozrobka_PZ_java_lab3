package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionStreamAnalyzerTest {

    private static final LocalDateTime START = LocalDateTime.of(2026, 1, 1, 12, 0);

    private TransactionStreamAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new TransactionStreamAnalyzer();
    }

    private static Transaction tx(int id, String amount) {
        return new Transaction("TX-" + id, "electronics", amount == null ? null : new BigDecimal(amount),
                START.plusMinutes(id));
    }

    /** Нескінченний стрім транзакцій із сумами 1, 2, 3, ... */
    private static Stream<Transaction> infiniteIncreasing() {
        return Stream.iterate(1, i -> i + 1).map(i -> tx(i, String.valueOf(i)));
    }

    /** Нескінченний стрім, який рахує, скільки елементів з нього реально прочитано. */
    private static Stream<Transaction> countingInfinite(AtomicInteger pulled, String amount) {
        return Stream.generate(() -> tx(pulled.incrementAndGet(), amount));
    }

    private static List<String> ids(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::transactionId).toList();
    }

    @Nested
    @DisplayName("findFirstHighValueTransaction")
    class FindFirstHighValue {

        @Test
        @Timeout(2)
        @DisplayName("знаходить першу транзакцію над порогом у нескінченному стрімі")
        void findsInInfiniteStream() {
            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(
                    infiniteIncreasing(), new BigDecimal("1000"));

            assertEquals("TX-1001", result.map(Transaction::transactionId).orElse("none"));
        }

        @Test
        @Timeout(2)
        @DisplayName("short-circuit: читає з нескінченного джерела лише до першого збігу")
        void stopsPullingAfterMatch() {
            AtomicInteger pulled = new AtomicInteger();
            Stream<Transaction> stream = Stream.generate(() -> {
                int id = pulled.incrementAndGet();
                return tx(id, id == 5 ? "500.00" : "10.00");
            });

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));

            assertAll(
                    () -> assertTrue(result.isPresent()),
                    () -> assertEquals(5, pulled.get()));
        }

        @Test
        @DisplayName("поріг строгий: сума, що дорівнює порогу, не підходить")
        void thresholdIsExclusive() {
            Stream<Transaction> stream = Stream.of(tx(1, "100"), tx(2, "100.00"), tx(3, "100.01"));

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));

            assertEquals(Optional.of("TX-3"), result.map(Transaction::transactionId));
        }

        @Test
        @DisplayName("повертає Optional.empty() для скінченного стріму без збігів")
        void emptyWhenNoMatchInFiniteStream() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "2"));

            assertTrue(analyzer.findFirstHighValueTransaction(stream, new BigDecimal("50")).isEmpty());
        }

        @Test
        @DisplayName("повертає Optional.empty() для порожнього стріму")
        void emptyForEmptyStream() {
            assertTrue(analyzer.findFirstHighValueTransaction(Stream.empty(), BigDecimal.ONE).isEmpty());
        }

        @Test
        @DisplayName("пропускає null-транзакції та транзакції без суми")
        void skipsNullsSafely() {
            Stream<Transaction> stream = Stream.of(null, tx(1, null), tx(2, "999"));

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, BigDecimal.TEN);

            assertEquals(Optional.of("TX-2"), result.map(Transaction::transactionId));
        }

        @Test
        @DisplayName("кидає IllegalArgumentException для null-аргументів")
        void rejectsNullArguments() {
            assertAll(
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.ONE)),
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null)));
        }
    }

    @Nested
    @DisplayName("takeTransactionsUntilBudgetExceeded")
    class TakeUntilBudgetExceeded {

        @Test
        @Timeout(2)
        @DisplayName("зупиняється на нескінченному стрімі, щойно бюджет був би перевищений")
        void stopsOnInfiniteStream() {
            // 1 + 2 + 3 + 4 = 10 <= 10; з п'ятою транзакцією було б 15 > 10
            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                    infiniteIncreasing(), new BigDecimal("10"));

            assertEquals(List.of("TX-1", "TX-2", "TX-3", "TX-4"), ids(result));
        }

        @Test
        @Timeout(2)
        @DisplayName("short-circuit: читає лише одну зайву транзакцію з нескінченного джерела")
        void pullsOnlyOneExtraElement() {
            AtomicInteger pulled = new AtomicInteger();

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                    countingInfinite(pulled, "25.00"), new BigDecimal("100"));

            assertAll(
                    () -> assertEquals(4, result.size()),
                    () -> assertEquals(5, pulled.get()));
        }

        @Test
        @DisplayName("транзакція, що рівно вичерпує бюджет, включається")
        void exactBudgetIsIncluded() {
            Stream<Transaction> stream = Stream.of(tx(1, "30"), tx(2, "70"), tx(3, "0.01"));

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1", "TX-2"), ids(result));
        }

        @Test
        @DisplayName("повертає порожній список, якщо вже перша транзакція перевищує бюджет")
        void emptyWhenFirstExceeds() {
            Stream<Transaction> stream = Stream.of(tx(1, "500"), tx(2, "1"));

            assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100")).isEmpty());
        }

        @Test
        @DisplayName("не продовжує брати транзакції після першого перевищення")
        void doesNotResumeAfterExceeding() {
            // 60 вміщується, 50 — ні, 10 вмістилося б, але обробка вже зупинена
            Stream<Transaction> stream = Stream.of(tx(1, "60"), tx(2, "50"), tx(3, "10"));

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1"), ids(result));
        }

        @Test
        @DisplayName("повертає весь скінченний стрім, якщо бюджет не перевищено")
        void returnsWholeFiniteStream() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3"));

            assertEquals(3, analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100")).size());
        }

        @Test
        @DisplayName("вважає відсутню суму нулем і пропускає null-елементи")
        void nullAmountCountsAsZero() {
            Stream<Transaction> stream = Stream.of(tx(1, "40"), null, tx(2, null), tx(3, "60"), tx(4, "1"));

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1", "TX-2", "TX-3"), ids(result));
        }

        @Test
        @DisplayName("результат незмінний")
        void resultIsUnmodifiable() {
            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                    Stream.of(tx(1, "1")), BigDecimal.TEN);

            assertThrows(UnsupportedOperationException.class, () -> result.add(tx(2, "1")));
        }

        @Test
        @DisplayName("кидає IllegalArgumentException для null-аргументів і від'ємного бюджету")
        void rejectsInvalidArguments() {
            assertAll(
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.ONE)),
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null)),
                    () -> assertThrows(IllegalArgumentException.class,
                            () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), new BigDecimal("-1"))));
        }
    }

    @Nested
    @DisplayName("processBatches (кастомний Gatherer)")
    class ProcessBatches {

        @Test
        @Timeout(2)
        @DisplayName("формує рівно maxBatches батчів з нескінченного стріму")
        void batchesInfiniteStream() {
            List<List<Transaction>> batches = analyzer.processBatches(infiniteIncreasing(), 3, 2);

            assertAll(
                    () -> assertEquals(2, batches.size()),
                    () -> assertEquals(List.of("TX-1", "TX-2", "TX-3"), ids(batches.get(0))),
                    () -> assertEquals(List.of("TX-4", "TX-5", "TX-6"), ids(batches.get(1))));
        }

        @Test
        @Timeout(2)
        @DisplayName("short-circuit: читає з джерела рівно batchSize * maxBatches елементів")
        void pullsExactlyNeededElements() {
            AtomicInteger pulled = new AtomicInteger();

            analyzer.processBatches(countingInfinite(pulled, "1.00"), 4, 5);

            assertEquals(20, pulled.get());
        }

        @Test
        @Timeout(2)
        @DisplayName("батчі розміром 1")
        void singleElementBatches() {
            List<List<Transaction>> batches = analyzer.processBatches(infiniteIncreasing(), 1, 3);

            assertEquals(List.of(List.of("TX-1"), List.of("TX-2"), List.of("TX-3")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @Timeout(2)
        @DisplayName("працює разом з іншими ледачими операціями нескінченного стріму")
        void composesWithOtherLazyOperations() {
            Stream<Transaction> evenOnly = infiniteIncreasing()
                    .filter(t -> t.amount().intValue() % 2 == 0);

            List<List<Transaction>> batches = analyzer.processBatches(evenOnly, 2, 2);

            assertEquals(List.of(List.of("TX-2", "TX-4"), List.of("TX-6", "TX-8")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("повертає останній неповний батч, якщо скінченний стрім закінчився")
        void finiteStreamWithPartialBatch() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"), tx(4, "1"), tx(5, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("TX-1", "TX-2"), List.of("TX-3", "TX-4"), List.of("TX-5")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("не додає неповний батч, якщо ліміт батчів уже досягнуто")
        void noPartialBatchAfterLimit() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 1);

            assertEquals(List.of(List.of("TX-1", "TX-2")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("повертає порожній список для порожнього стріму")
        void emptyStream() {
            assertTrue(analyzer.processBatches(Stream.empty(), 3, 3).isEmpty());
        }

        @Test
        @Timeout(2)
        @DisplayName("батчі незмінні")
        void batchesAreUnmodifiable() {
            List<List<Transaction>> batches = analyzer.processBatches(infiniteIncreasing(), 2, 1);

            assertThrows(UnsupportedOperationException.class, () -> batches.get(0).add(tx(99, "1")));
        }

        @Test
        @Timeout(2)
        @DisplayName("Gatherer поважає downstream short-circuit (limit після gather)")
        void respectsDownstreamShortCircuit() {
            AtomicInteger pulled = new AtomicInteger();

            List<List<Transaction>> batches = countingInfinite(pulled, "1")
                    .gather(TransactionStreamAnalyzer.limitedBatches(3, Integer.MAX_VALUE))
                    .limit(2)
                    .toList();

            assertAll(
                    () -> assertEquals(2, batches.size()),
                    () -> assertFalse(pulled.get() > 7, "прочитано забагато елементів: " + pulled.get()));
        }

        @Test
        @DisplayName("кидає IllegalArgumentException для null-стріму")
        void rejectsNullStream() {
            assertThrows(IllegalArgumentException.class, () -> analyzer.processBatches(null, 1, 1));
        }

        @ParameterizedTest(name = "batchSize = {0}")
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        @DisplayName("кидає IllegalArgumentException, якщо batchSize <= 0")
        void rejectsNonPositiveBatchSize(int batchSize) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(infiniteIncreasing(), batchSize, 1));
        }

        @ParameterizedTest(name = "maxBatches = {0}")
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        @DisplayName("кидає IllegalArgumentException, якщо maxBatches <= 0")
        void rejectsNonPositiveMaxBatches(int maxBatches) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(infiniteIncreasing(), 1, maxBatches));
        }
    }
}
