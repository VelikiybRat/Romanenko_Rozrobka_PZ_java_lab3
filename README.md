# Лабораторна робота №3

**Дисципліна:** Технології розробки на платформі Java (2026)

**Тема:** Опрацювання нескінченних послідовностей даних за допомогою ледачих (lazy) операцій Java Stream API, безпечне керування наявністю значень через `Optional` та створення кастомних проміжних операцій за допомогою Stream Gatherers.

**Мета:** Опанувати концепції нескінченних потоків (`Stream.generate`, `Stream.iterate`), навчитися контролювати виконання за допомогою методів короткого замикання (`findFirst`, `takeWhile`), уникати блокування при роботі з нескінченними джерелами та реалізувати власну stateful проміжну операцію через `java.util.stream.Gatherer`.

**Виконав:** Романенко Іван Євгенович

**Група:** ІО-45

## Визначення варіанта

Варіант визначається остачею від ділення номера залікової книжки на 3.

- Номер залікової книжки: **4509**.
- Формула: `V = N mod 3`.
- Обчислення: `4509 mod 3 = 0` (4509 = 3 · 1503).

**Варіант 0 — електронна комерція та стрім транзакцій (`TransactionStreamAnalyzer`).**

## Опис реалізації

Усі класи розміщено в пакеті `ua.kpi.comsys.streams`.

### Модель даних

```java
public record Transaction(String transactionId, String category,
                          BigDecimal amount, LocalDateTime timestamp) {}
```

### `TransactionStreamAnalyzer`

| Метод | Механізм | Поведінка |
|---|---|---|
| `findFirstHighValueTransaction(stream, threshold)` | `filter` + `findFirst` (short-circuit) | Повертає `Optional` першої транзакції з `amount > threshold`; `Optional.empty()`, якщо скінченний стрім не містить такої. |
| `takeTransactionsUntilBudgetExceeded(stream, maxBudget)` | `takeWhile` з накопичувачем суми | Бере транзакції, поки накопичена сума `<= maxBudget`; на першій транзакції, з якою бюджет було б перевищено, обробка зупиняється. |
| `processBatches(stream, batchSize, maxBatches)` | власний `Gatherer.ofSequential` | Групує елементи у вікна по `batchSize` і зупиняє обробку після рівно `maxBatches` батчів. |

**Кастомний Gatherer.** Метод `limitedBatches(batchSize, maxBatches)` будує послідовний `Gatherer` з трьох частин:

1. **initializer** створює стан `BatchState`: поточний незаповнений батч і лічильник уже випущених батчів;
2. **integrator** додає елемент до поточного батчу. Коли батч заповнено, передає його далі (`downstream.push`) і повертає `false`, щойно сформовано `maxBatches` батчів. Це сигнал короткого замикання: upstream (навіть нескінченний `Stream.generate`) більше не опитується;
3. **finisher** для скінченного стріму передає останній неповний батч, якщо ліміт ще не досягнуто.

Gatherer також враховує `downstream.isRejecting()`, тобто коректно працює з подальшими short-circuit операціями (наприклад, `limit`). Батчі повертаються як незмінні списки (`List.copyOf`).

**Робота з `Optional` без антипатернів.** У коді немає `Optional.get()`. Відсутність суми обробляється функціонально: `Optional.ofNullable(t.amount()).map(...).orElse(false)` при пошуку та `.orElse(BigDecimal.ZERO)` при підрахунку бюджету.

**Валідація.** Якщо `stream` або обов'язковий аргумент дорівнює `null`, кидається `IllegalArgumentException`. Також виняток виникає при `batchSize <= 0`, `maxBatches <= 0` і від'ємному `maxBudget`. `null`-елементи у стрімі пропускаються.

> Примітка: для нескінченного стріму `takeTransactionsUntilBudgetExceeded` завершується, лише якщо суми додатні. Нескінченний потік нульових транзакцій ніколи не перевищить бюджет, і це властивість самої задачі.

## Структура проєкту

```text
Romanenko_Rozrobka_PZ_java_lab3/
├── gradle/
│   ├── libs.versions.toml          # Version Catalog
│   └── wrapper/
├── src/
│   ├── main/java/ua/kpi/comsys/streams/
│   │   ├── Transaction.java
│   │   └── TransactionStreamAnalyzer.java
│   └── test/java/ua/kpi/comsys/streams/
│       └── TransactionStreamAnalyzerTest.java
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
├── gradlew / gradlew.bat
└── README.md
```

## Середовище та технології

- **Java:** JDK 25 (Stream Gatherers — стандартний API починаючи з Java 24; версія задається в `libs.versions.toml`).
- **Система збирання:** Gradle 9.1 (Gradle Wrapper), Kotlin DSL.
- **Керування залежностями:** Version Catalog `gradle/libs.versions.toml`.
- **Тестування:** JUnit Jupiter 5.11.4 (через JUnit BOM).
- **Покриття:** JaCoCo 0.8.15.

## Запуск збірки та тестів

Потрібен встановлений JDK 25. Gradle окремо встановлювати не потрібно, бо використовується Wrapper.

```bash
# Linux / macOS
./gradlew build
./gradlew test

# Windows
gradlew.bat build
gradlew.bat test
```

Після `test` автоматично формується звіт JaCoCo: `build/reports/jacoco/test/html/index.html`.
Звіт JUnit: `build/reports/tests/test/index.html`.

## Тестування

У тестах (`TransactionStreamAnalyzerTest`) як джерела використано **нескінченні стріми** `Stream.iterate(...)` і `Stream.generate(...)`. Усі тести з нескінченними джерелами позначено `@Timeout(2)`, що підтверджує відсутність зависань.

Крім результату, тести перевіряють і **кількість реально прочитаних елементів** (лічильник `AtomicInteger` у `Stream.generate`):

- `findFirstHighValueTransaction` читає джерело лише до першого збігу;
- `takeTransactionsUntilBudgetExceeded` читає рівно на один елемент більше, ніж повертає;
- `processBatches` читає рівно `batchSize · maxBatches` елементів.

Також перевіряються граничні випадки:

- строгий поріг (`amount == threshold` не підходить);
- транзакція, що рівно вичерпує бюджет;
- відсутність «відновлення» після перевищення бюджету;
- неповний останній батч скінченного стріму;
- комбінація Gatherer-а з `filter` і `limit`;
- незмінність результатів;
- `IllegalArgumentException` для всіх некоректних аргументів (у тому числі `@ParameterizedTest` для `batchSize`/`maxBatches <= 0`).

## Висновок

Під час виконання лабораторної роботи реалізовано сервіс аналітики транзакцій, який безпечно працює з нескінченними потоками завдяки ледачим операціям і короткому замиканню (`findFirst`, `takeWhile`). Створено власну stateful проміжну операцію на основі `Gatherer.ofSequential`, яка сама зупиняє нескінченне джерело після потрібної кількості батчів. Відсутні значення оброблено через `Optional` без виклику `get()`. Коректність і відсутність зависань перевірено модульними тестами JUnit 5 з `@Timeout`.
