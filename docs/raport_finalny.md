# Raport Finalny

**NYC Weather vs Taxi Trips — Hurtownia Danych**

Maksim Razantsau, Oleksii Vinichenko

---

## 1. Cel systemu oraz planowane korzyści

### 1.1 Cel

Celem systemu jest zaprojektowanie i wdrożenie hurtowni danych integrującej informacje o przejazdach nowojorskich taksówek (TLC Trip Record Data) z historycznymi danymi pogodowymi (Open-Meteo). Zastosowano model konstelacji faktów, który umożliwia niezależną analizę zarówno zdarzeń transportowych, jak i warunków atmosferycznych oraz ich wzajemnych korelacji.

### 1.2 Planowane korzyści dla odbiorcy

- **Identyfikacja korelacji** między warunkami pogodowymi a popytem na taksówki — liczba przejazdów w deszczu vs. przy bezchmurnym niebie.
- **Analiza średnich czasów przejazdów i opłat** w różnych warunkach atmosferycznych — wpływ śniegu i mrozu na czas przejazdu i całkowity koszt.
- **Optymalizacja liczby aktywnych pojazdów** w zależności od bieżącej i prognozowanej pogody — redukcja pustych przebiegów.
- **Możliwość agregacji danych pogodowych niezależnie** od szczegółowych przejazdów — analiza trendów temperaturowych i opadów w NYC.
- **Wsparcie decyzyjne w zakresie dynamicznej polityki cenowej** (surge pricing) — korelacja podaży i popytu z warunkami atmosferycznymi.
- **Dashboardy interaktywne w Tableau** — gotowy model danych umożliwia analizę ad-hoc przez analityków biznesowych.

---

## 2. Diagram i opis finalnej architektury rozwiązania

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ŹRÓDŁA DANYCH                               │
│  TLC Parquet (AWS) ──────┐                                          │
│  TLC Zone CSV (AWS) ─────┼──▶  PySpark ETL (Docker: etl-runner)    │
│  Open-Meteo API ─────────┘       src/ingest/  src/transform/       │
└──────────────────────────────────────────┬──────────────────────────┘
                                           │
                    ┌──────────────────────▼──────────────────────────┐
                    │           PostgreSQL 15 (Docker: postgres-dwh)   │
                    │                                                   │
                    │  staging.*              dwh.*                    │
                    │  ┌──────────────┐      ┌─────────────────────┐  │
                    │  │ fact_trip    │      │ dim_date            │  │
                    │  │ fact_weather │  ──▶ │ dim_time            │  │
                    │  │ zone_lookup  │      │ dim_location        │  │
                    │  └──────────────┘      │ dim_weather_type    │  │
                    │   (strefa lądowania)   │ fact_trip           │  │
                    │                        │ fact_weather        │  │
                    │                        └─────────────────────┘  │
                    │                         (konstelacja faktów)    │
                    └──────────────────────┬──────────────────────────┘
                                           │ TCP :5432
                    ┌──────────────────────▼──────────────────────────┐
                    │           Tableau Desktop                        │
                    │           (bezpośrednie połączenie z dwh.*)     │
                    └─────────────────────────────────────────────────┘
```

### Opis warstw

| Warstwa | Technologia | Rola |
|---|---|---|
| Źródła danych | AWS CloudFront (Parquet/CSV), Open-Meteo REST API | Dostarczanie surowych danych |
| ETL | PySpark 4.1.1, psycopg2, Docker Compose | Ekstrakcja, czyszczenie, transformacja, ładowanie |
| Strefa lądowania | PostgreSQL 15, schemat `staging` | Bufor surowych danych przed transformacją |
| Hurtownia | PostgreSQL 15, schemat `dwh` | Model konstelacji faktów, docelowy model analityczny |
| BI | Tableau Desktop | Zapytania ad-hoc, raporty, dashboardy |

Schemat `staging` pełni rolę bufora lądowania — surowe dane ze źródeł trafiają najpierw tutaj, a dopiero po transformacji i czyszczeniu zasilają schemat `dwh`. Oddzielenie warstw umożliwia niezależny re-run transformacji bez ponownego pobierania danych.

---

## 3. Wykorzystywane zbiory danych

| Zbiór | Dostęp | Format | Częstotliwość |
|---|---|---|---|
| TLC Trip Record Data | Publiczny (AWS CloudFront) | Parquet | Miesięczne (~2 mies. opóźnienia) |
| TLC Taxi Zone Lookup | Publiczny (AWS CloudFront) | CSV | Rzadko (nowe strefy) |
| Open-Meteo Historical Forecast API | REST API (bezpłatny) | JSON (15 min) | Codzienne |

### Szczegóły źródeł

**TLC Trip Record Data** — pliki Parquet publikowane miesięcznie przez NYC Taxi & Limousine Commission. Każdy plik zawiera wszystkie przejazdy żółtych taksówek w danym miesiącu (ok. 3 mln rekordów na miesiąc). URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{RRRR-MM}.parquet`

**TLC Taxi Zone Lookup** — słownik referencyjny mapujący TLC LocationID (265 stref) na nazwę dzielnicy, strefy i kategorii obsługi. URL: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

**Open-Meteo Historical Forecast API** — bezpłatne API historycznych danych pogodowych z rozdzielczością 15 minut. Endpoint: `https://historical-forecast-api.open-meteo.com/v1/forecast`. Pobierane zmienne: `temperature_2m` (°C), `precipitation` (mm), `weathercode` (kod WMO), `windspeed_10m` (km/h). Uwaga: standardowy endpoint archiwum (`archive-api.open-meteo.com`) nie obsługuje rozdzielczości 15-minutowej — do uzyskania danych `minutely_15` wymagany jest endpoint `historical-forecast-api`.

---

## 4. Opis procesu ETL

### 4.1 Ekstrakcja (`src/ingest/`)

**Dane taksówkowe** (`taxi_ingest.py`):
- Strumieniowe pobieranie miesięcznego pliku Parquet HTTP (chunk 1 MB, timeout 120 s, zapis do `/tmp`)
- Odczyt do DataFrame PySpark, zliczenie rekordów surowych (styczeń 2023: **3 066 766**)

**Dane pogodowe** (`weather_ingest.py`):
- Żądanie HTTP z mechanizmem ponowień: 3 próby, `backoff_factor=1`, kody błędów: 429/500/502/503/504
- Walidacja struktury odpowiedzi: sprawdzenie pola `minutely_15` i wszystkich 4 wymaganych kolumn
- Konwersja JSON → pandas DataFrame → lokalny plik Parquet → DataFrame PySpark
- Styczeń 2023: **2 976 obserwacji 15-minutowych**

**Słownik stref** (`zone_ingest.py`):
- Pobieranie CSV, normalizacja nazw kolumn
- Pełne odświeżenie przy każdym uruchomieniu: `TRUNCATE staging.zone_lookup` przed załadowaniem
- Załadowano **265 stref**

### 4.2 Reguły czyszczenia danych TLC

Zaimplementowane filtry w `taxi_ingest.py` (stosowane w podanej kolejności):

| Nr | Reguła | Uzasadnienie | Wpływ (sty 2023) |
|---|---|---|---|
| 1 | `total_amount > 0` | Usunięcie anulowanych / testowych przejazdów | — |
| 2 | `trip_distance > 0` | Usunięcie przejazdów o zerowym dystansie | — |
| 3 | `passenger_count > 0` | Usunięcie rekordów bez pasażerów | — |
| 4 | `tpep_pickup_datetime` w granicach nominalnego miesiąca | Pliki TLC zawierają rekordy z poprzednich/następnych miesięcy; naruszają idempotentność ładowania przyrostowego | **84 rekordy** usunięte |
| 5 | `dropDuplicates()` po 19 kolumnach | Pliki TLC mogą zawierać identyczne wiersze | **2 rekordy** usunięte (1 para) |

Wynik po czyszczeniu: **2 884 355 rekordów** (93,9% rekordów surowych).

### 4.3 Standaryzacja kolumn

Parquet TLC używa nazw (`VendorID`, `PULocationID`), schemat staging używa (`vendor_id`, `pu_location_id`). Mapowanie (`COLUMN_RENAMES` w `taxi_ingest.py`):

| Oryginalna kolumna | Kolumna po standaryzacji |
|---|---|
| VendorID | vendor_id |
| RatecodeID | rate_code_id |
| PULocationID | pu_location_id |
| DOLocationID | do_location_id |
| Airport_fee | airport_fee |

### 4.4 Transformacja do modelu konstelacji (`src/transform/`)

### Tworzenie kluczy daty i czasu (`pipeline.py`)

* **`date_key`**: Data zamieniona na liczbę w formacie **RRRRMMDD** (np. `20230115` oznacza 15 stycznia 2023 r.).
* **`time_key`**: Czas zamieniony na liczbę w formacie **GGMM00** (np. `143000` oznacza godzinę 14:30). 
  * *Ważne:* Sekundy zawsze sztucznie wyzerowujemy (końcówka `00`). Dzięki temu o wiele łatwiej jest później łączyć tabele z różnymi danymi (np. przypisać pogodę z danej godziny do konkretnego przejazdu).

### Budowa tabeli przejazdów (`fact_trip`)
* **Czas trwania przejazdu:** Wyliczany w sekundach (różnica między końcem a początkiem kursu). Odrzucamy błędne rekordy, gdzie czas wynosi zero lub jest ujemny.
* **Lokalizacje:** Przypisujemy strefy startowe i końcowe do przejazdu na podstawie naszego słownika (`dim_location`). Jeśli system nie potrafi rozpoznać strefy, pole pozostaje puste (nie blokuje to wczytania przejazdu).
* **Liczba pasażerów:** Zapisywana po prostu jako liczba całkowita.

### Budowa tabeli pogodowej (`fact_weather`)
* **Uśrednianie do pełnych godzin:** Surowe dane pogodowe są zbierane co 15 minut. My łączymy je tak, aby uzyskać jeden, czysty odczyt dla każdej godziny.
* **Kalkulacja metryk:** W ramach danej godziny wyciągamy średnią temperaturę i prędkość wiatru, a opady deszczu sumujemy.
* **Dominująca pogoda:** Zbieramy najczęstszy stan pogody w danej godzinie (jeśli np. przez większość godziny padało, oznaczamy całą godzinę jako deszczową).
* **Wynik:** Testujemy, na przykład, dla stycznia i lutego (59 dni × 24 godziny). Otrzymujemy dokładnie 1 416 wierszy, co pokrywa każdą godzinę.

### Ładowanie wymiarów (`dimensions.py`)
* **Tabela dat (`dim_date`):** Tworzy jeden wpis na każdy dzień kalendarzowy. Dodatkowo automatycznie wykrywa i oznacza amerykańskie święta (dla stycznia i lutego 2023 r. zidentyfikowano ich 7).
* **Tabela lokalizacji (`dim_location`):** Ładuje oficjalną listę stref taksówkowych. Jeśli w surowych danych brakuje nazwy dzielnicy lub strefy, system bezpiecznie zastępuje pustkę słowem "Unknown" (Nieznane).

### 4.5 Scenariusze ładowania (`main.py --mode`)

System obsługuje dwa tryby wywołane przez flagę `--mode`:

**`--mode init` (inicjalizacja):**
1. `TRUNCATE staging.fact_trip, staging.fact_weather`
2. `TRUNCATE dwh.fact_trip, dwh.fact_weather`
3. Pełne pobranie i załadowanie wskazanego okresu
4. Bezpieczny wielokrotny re-run — każde wywołanie daje ten sam wynik

**`--mode incremental` (kolejna iteracja):**
1. `DELETE FROM staging.fact_trip WHERE tpep_pickup_datetime >= start AND < end+1 dzień`
2. `DELETE FROM staging.fact_weather WHERE time >= start AND < end+1 dzień`
3. `DELETE FROM dwh.fact_trip WHERE date_key BETWEEN start_key AND end_key`
4. `DELETE FROM dwh.fact_weather WHERE date_key BETWEEN start_key AND end_key`
5. Append danych dla wskazanego okresu
6. Idempotentny — wielokrotny re-run dla tego samego okresu nie tworzy duplikatów

```bash
# Inicjalizacja
docker compose exec etl-runner uv run python main.py \
  --year 2023 --start-month 1 --end-month 1 --mode init

# Kolejna iteracja
docker compose exec etl-runner uv run python main.py \
  --year 2023 --start-month 2 --end-month 2 --mode incremental
```

---

## 5. Model fizyczny hurtowni danych

### Diagram ER (konstelacja faktów)

![diagram](docs/diagram.png)

### Szczegółowe definicje tabel

#### staging.fact_trip — surowe dane TLC
| Kolumna | Typ | Opis |
|---|---|---|
| vendor_id | INTEGER | Identyfikator dostawcy |
| tpep_pickup_datetime | TIMESTAMP | Data i czas podjęcia pasażera |
| tpep_dropoff_datetime | TIMESTAMP | Data i czas wysadzenia pasażera |
| passenger_count | REAL | Liczba pasażerów |
| trip_distance | REAL | Dystans przejazdu (mile) |
| rate_code_id | REAL | Kod taryfy |
| store_and_fwd_flag | TEXT | Flaga trybu zapisu |
| pu_location_id | INTEGER | ID strefy podjęcia |
| do_location_id | INTEGER | ID strefy wysadzenia |
| payment_type | INTEGER | Typ płatności |
| fare_amount | REAL | Opłata podstawowa (USD) |
| extra | REAL | Dopłaty (USD) |
| mta_tax | REAL | Podatek MTA (USD) |
| tip_amount | REAL | Napiwek (USD) |
| tolls_amount | REAL | Opłaty drogowe (USD) |
| improvement_surcharge | REAL | Dopłata za ulepszenia (USD) |
| total_amount | REAL | Całkowita opłata (USD) |
| congestion_surcharge | REAL | Dopłata za korki (USD) |
| airport_fee | REAL | Opłata lotniskowa (USD) |
| ingested_at | TIMESTAMP | Znacznik czasu załadowania |

#### staging.fact_weather — surowe obserwacje 15-minutowe
| Kolumna | Typ | Opis |
|---|---|---|
| time | TIMESTAMP | Czas obserwacji (co 15 min) |
| temperature_2m | REAL | Temperatura na 2 m n.p.g. (°C) |
| precipitation | REAL | Suma opadów (mm) |
| weathercode | INTEGER | Kod pogody WMO |
| windspeed_10m | REAL | Prędkość wiatru na 10 m (km/h) |
| ingested_at | TIMESTAMP | Znacznik czasu załadowania |

#### staging.zone_lookup — słownik stref TLC
| Kolumna | Typ | Opis |
|---|---|---|
| location_id | INTEGER | Identyfikator strefy TLC |
| borough | TEXT | Dzielnica (Manhattan, Brooklyn, …) |
| zone | TEXT | Nazwa strefy |
| service_zone | TEXT | Kategoria obsługi |
| ingested_at | TIMESTAMP | Znacznik czasu załadowania |

#### dwh.dim_date — wymiar daty (PK: date_key = YYYYMMDD)
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| date_key | BIGINT | PK NOT NULL | Klucz YYYYMMDD |
| full_date | DATE | UNIQUE NOT NULL | Pełna data |
| year | INT | NOT NULL | Rok |
| month | INT | NOT NULL | Miesiąc |
| day | INT | NOT NULL | Dzień |
| holiday_flag | BIT | NOT NULL DEFAULT 0 | Czy święto US/NY |
| holiday_name | VARCHAR(100) | NULL | Nazwa święta |

#### dwh.dim_time — wymiar czasu (PK: time_key = HHMMSS, 1440 wierszy)
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| time_key | BIGINT | PK NOT NULL | Klucz HHMMSS (SS=00) |
| hour | BIGINT | NOT NULL | Godzina (0–23) |
| minute | TINYINT | NOT NULL | Minuta (0–59) |
| time_of_day | VARCHAR(20) | NOT NULL | Night/Morning/Afternoon/Evening |

Przedziały `time_of_day`: Night (0–5), Morning (6–11), Afternoon (12–17), Evening (18–23).
Wiersz seeded statycznie przez `schema.sql` przy każdym `db-init` (1440 wierszy = 24h × 60 min).

#### dwh.dim_location — wymiar lokalizacji (PK: location_key = TLC LocationID)
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| location_key | BIGINT | PK NOT NULL | Klucz = TLC LocationID |
| location_id | INT | NOT NULL | TLC LocationID |
| borough | VARCHAR(60) | NOT NULL | Dzielnica |
| zone | VARCHAR(100) | NOT NULL | Strefa |
| service_zone | VARCHAR(60) | NOT NULL | Kategoria obsługi |

Załadowanych **265 stref** (rozkład: Manhattan 69, Queens 69, Brooklyn 61, Bronx 43, Staten Island 20, EWR 1, Unknown 2).

#### dwh.dim_weather_type — wymiar typów pogody (PK: weather_type_key = kod WMO, 28 wierszy)
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| weather_type_key | BIGINT | PK NOT NULL | Kod WMO |
| condition_name | VARCHAR(100) | NOT NULL | Grupa (Clear, Rain, Snow, …) |
| description | VARCHAR(200) | NULL | Opis szczegółowy |

Seeded statycznie przez `schema.sql`. Przykłady: 0=Clear sky, 61=Slight rain, 71=Slight snowfall, 95=Thunderstorm.

#### dwh.fact_trip — fakt przejazdów (ziarno: 1 wiersz = 1 przejazd)
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| date_key | BIGINT | FK → dim_date NOT NULL | Klucz daty podjęcia |
| time_key | BIGINT | FK → dim_time NOT NULL | Klucz czasu podjęcia |
| pu_location_key | BIGINT | FK → dim_location NULL | Strefa podjęcia |
| do_location_key | BIGINT | FK → dim_location NULL | Strefa wysadzenia |
| trip_distance | DECIMAL(8,2) | NOT NULL | Dystans (mile) |
| fare_amount | MONEY | NOT NULL | Opłata podstawowa (USD) |
| tip_amount | MONEY | NOT NULL | Napiwek (USD) |
| total_amount | MONEY | NOT NULL | Całkowita opłata (USD) |
| trip_duration_sec | INT | NOT NULL | Czas przejazdu (s) |
| passenger_count | INT | NULL | Liczba pasażerów |
| loaded_at | DATETIME2 | DEFAULT GETDATE() | Znacznik załadowania |

Indeksy: `idx_fact_trip_date`, `idx_fact_trip_time`, `idx_fact_trip_pu_loc`, `idx_fact_trip_do_loc`.

#### dwh.fact_weather — fakt pogody (ziarno: 1 wiersz = 1 godzina; PK = (date_key, time_key))
| Kolumna | Typ | Wymagalność | Opis |
|---|---|---|---|
| date_key | BIGINT | PK FK → dim_date NOT NULL | Klucz daty |
| time_key | BIGINT | PK FK → dim_time NOT NULL | Klucz godziny (minuty=00) |
| weather_type_key | BIGINT | FK → dim_weather_type NOT NULL | Dominujący kod pogody |
| temperature | DECIMAL(5,2) | NOT NULL | Śr. temperatura (°C) |
| precipitation | DECIMAL(6,2) | NOT NULL | Suma opadów (mm) |
| wind_speed | DECIMAL(5,2) | NOT NULL | Śr. prędkość wiatru (km/h) |
| loaded_at | DATETIME2 | DEFAULT GETDATE() | Znacznik załadowania |

Indeks: `idx_fact_weather_date`.

---

## 6. Opis kluczowych miar, atrybutów i hierarchii

### Miary

| Miara | Tabela | Opis | Jednostka | Agregacja |
|---|---|---|---|---|
| trip_distance | fact_trip | Dystans przejazdu | mile | SUM, AVG |
| fare_amount | fact_trip | Opłata podstawowa | USD | SUM, AVG |
| tip_amount | fact_trip | Napiwek | USD | SUM, AVG |
| total_amount | fact_trip | Całkowita opłata | USD | SUM, AVG |
| trip_duration_sec | fact_trip | Czas przejazdu | sekundy | AVG, MIN, MAX |
| passenger_count | fact_trip | Liczba pasażerów | osoby | SUM, AVG |
| temperature | fact_weather | Temperatura (śr. z 15-min) | °C | AVG, MIN, MAX |
| precipitation | fact_weather | Suma opadów (agregat z 15-min) | mm | SUM, AVG |
| wind_speed | fact_weather | Prędkość wiatru (śr. z 15-min) | km/h | AVG, MAX |

### Hierarchie

**Hierarchia daty** (dim_date):
```
Year → Month → Day
              └── HolidayFlag (atrybut)
              └── HolidayName (atrybut)
```

**Hierarchia czasu** (dim_time):
```
TimeOfDay (Night/Morning/Afternoon/Evening) → Hour → Minute
```

**Hierarchia lokalizacji** (dim_location):
```
Borough → Zone → ServiceZone
```
Przykład: Manhattan → Midtown Center → Yellow Zone

**Hierarchia warunków pogodowych** (dim_weather_type):
```
ConditionName (Clear/Rain/Snow/…) → weather_type_key (WMO code)
```

### Kluczowe KPIs

- **Liczba przejazdów na dzień** — `COUNT(*) FROM fact_trip GROUP BY date_key`
- **Średnia opłata w danym typie pogody** — `AVG(total_amount) ... JOIN dim_weather_type`
- **Średni czas przejazdu wg pory dnia** — `AVG(trip_duration_sec) ... JOIN dim_time`
- **Suma opadów vs. liczba przejazdów** — blending fact_weather + fact_trip po date_key

---
## 7. Opis warstwy raportowej

### Dostęp do danych
Aplikacja Tableau Desktop łączy się bezpośrednio z bazą PostgreSQL poprzez standardowe połączenie TCP. Dane pobierane są w trybie Live / Extract (w zależności od potrzeb analitycznych).

| Parametr | Wartość |
|---|---|
| **Server** | `localhost` (lub adres hosta Docker) |
| **Port** | `5432` |
| **Database** | `nyc_weather_taxi` |
| **Schema** | `dwh` |
| **User** | `data_engineer` |

### Model biznesowy danych
W warstwie semantycznej Tableau zastosowano model relacyjny oparty na **Logical Layer** (Relacje zamiast fizycznych złączeń typu JOIN). Umożliwia to elastyczną analizę danych o różnym poziomie ziarnistości (przejazdy vs pogoda) bez ryzyka duplikacji danych.

Model biznesowy to klasyczna **Konstelacja Faktów (Galaxy Schema)**:
* **Fakty przejazdów** (`fact_trip`) i **Fakty pogodowe** (`fact_weather`) funkcjonują jako niezależne tabele centralne.
* Współdzielą one (Blend / Relacje) wymiary wspólne: `dim_date` oraz `dim_time`.
* Tabela `fact_trip` posiada dodatkowe relacje z `dim_location` (osobno dla strefy startowej i końcowej).
* Tabela `fact_weather` posiada dedykowaną relację z `dim_weather_type`.

### Hierarchie w warstwie raportowej
Aby ułatwić użytkownikom końcowym analizę typu "drill-down" (np. od lat do pojedynczych minut), w Tableau zdefiniowano następujące hierarchie nawigacyjne:
* **Data:** `Year` → `Month` → `Day`
* **Czas:** `Time of Day` → `Hour` → `Minute`
* **Lokalizacja:** `Borough` → `Zone` → `Service Zone`
* **Pogoda:** `Condition Name` → `Description`

### Transformacje w obrębie warstwy raportowej
Oprócz twardych danych z bazy `dwh`, w warstwie raportowej Tableau zdefiniowano dedykowane transformacje (Calculated Fields), które wzbogacają analizę bez obciążania bazy danych:

1.  **Miary biznesowe (KPIs):**
    * `[Tip Percentage]` = `SUM([tip_amount]) / SUM([fare_amount])` (Procent napiwku względem opłaty za kurs).
    * `[Avg Trip Revenue]` = `SUM([total_amount]) / COUNT([fact_trip])` (Średni przychód na przejazd).
    * `[Revenue per Mile]` = `SUM([total_amount]) / SUM([trip_distance])` (Rentowność jednej mili).
2.  **Kategoryzacje i Flagi (Wymiary wyliczane):**
    * `[Is Bad Weather]` = `IF [condition_name] IN ('Rain', 'Snow', 'Thunderstorm') THEN True ELSE False END` (Ułatwia szybkie filtrowanie pogody na dobrą/złą).
3.  **Formatowanie:** Automatyczne rzutowanie pól walutowych (np. `fare_amount`) na format `$ USD` oraz dystansu na `miles` z dokładnością do dwóch miejsc po przecinku.

## 8. Opis realizacji przykładowych raportów

Na podstawie modelu zaimplementowanego w `dwh.*` możliwe jest przygotowanie następujących raportów zdefiniowanych w raporcie wstępnym:

### Raport 1 — Wpływ opadów na liczbę przejazdów
**Wizualizacja:** wykres słupkowy / liniowy z filtrami dzielnicy i pory dnia

**Logika SQL:**
```sql
SELECT
    CASE
        WHEN w.precipitation = 0 THEN 'Brak opadów'
        WHEN w.precipitation < 2 THEN 'Lekkie opady'
        ELSE 'Intensywne opady'
    END AS precipitation_bucket,
    COUNT(t.*) AS trip_count,
    AVG(t.total_amount) AS avg_fare
FROM dwh.fact_trip t
JOIN dwh.dim_date d ON t.date_key = d.date_key
JOIN dwh.fact_weather w ON t.date_key = w.date_key
    AND (t.time_key / 10000) = (w.time_key / 10000)
JOIN dwh.dim_time dt ON t.time_key = dt.time_key
GROUP BY 1
ORDER BY 1;
```

### Raport 2 — Średnia temperatura a czas przejazdu
**Wizualizacja:** heatmapa (oś X: temperatura w przedziałach, oś Y: pora dnia)

**Logika SQL:**
```sql
SELECT
    ROUND(w.temperature)::int AS temp_rounded,
    dt.time_of_day,
    AVG(t.trip_duration_sec / 60.0) AS avg_duration_min
FROM dwh.fact_trip t
JOIN dwh.fact_weather w ON t.date_key = w.date_key
    AND (t.time_key / 10000) = (w.time_key / 10000)
JOIN dwh.dim_time dt ON t.time_key = dt.time_key
GROUP BY 1, 2;
```

### Raport 3 — Liczba przejazdów w kolejnych miesiącach (trend)
**Wizualizacja:** wykres liniowy, filtr: rok

**Logika SQL:**
```sql
SELECT d.year, d.month, COUNT(*) AS trip_count
FROM dwh.fact_trip t
JOIN dwh.dim_date d ON t.date_key = d.date_key
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Raport 4 — Dashboard operacyjny (KPI dzienny)
**Wizualizacja:** karty KPI + mapa stref

**Metryki:**
- Dzienna liczba przejazdów: `COUNT(*) GROUP BY date_key`
- Suma opadów: `SUM(precipitation) GROUP BY date_key` z `fact_weather`
- Średni napiwek: `AVG(tip_amount) GROUP BY date_key`
- Liczba przejazdów na strefę: `COUNT(*) GROUP BY pu_location_key` + join `dim_location`

### Raport 5 — Porównanie sezonowe (zima vs. lato)
**Wizualizacja:** wykresy grupowane

**Logika SQL:**
```sql
SELECT
    CASE WHEN d.month IN (12,1,2) THEN 'Zima'
         WHEN d.month IN (6,7,8) THEN 'Lato'
         ELSE 'Wiosna/Jesień' END AS season,
    AVG(t.total_amount) AS avg_fare,
    AVG(t.trip_duration_sec/60.0) AS avg_duration_min,
    COUNT(*) AS trip_count
FROM dwh.fact_trip t
JOIN dwh.dim_date d ON t.date_key = d.date_key
GROUP BY 1;
```

### Raport 6 — Raport pogodowy godzinowy
**Wizualizacja:** wykresy liniowe temperatury, opadów i wiatru w czasie

**Logika SQL:**
```sql
SELECT
    d.full_date, dt.hour,
    w.temperature, w.precipitation, w.wind_speed,
    wt.condition_name
FROM dwh.fact_weather w
JOIN dwh.dim_date d ON w.date_key = d.date_key
JOIN dwh.dim_time dt ON w.time_key = dt.time_key
JOIN dwh.dim_weather_type wt ON w.weather_type_key = wt.weather_type_key
ORDER BY d.full_date, dt.hour;
```

---

## 9. Opis rezultatów analizy jakości danych

### Narzędzie i metodologia

Moduł jakości danych: `src/quality/checks.py`. Uruchomienie: `uv run python -m src.quality.checks`. Wyniki zapisywane do `reports/quality_report.{md,json}`. Zero na wyjściu w testach poza sprawdzeniem liczebości oznacza, ze test przeszedł.

### Wykryte i naprawione problemy

Podczas wdrożenia i pierwszego załadowania danych (2026-06-11) zidentyfikowano i naprawiono 2 problemy w danych źródłowych:

**Problem 1 — Rekordy spoza nominalnego miesiąca w plikach TLC:**
- Opis: Plik `yellow_tripdata_2023-01.parquet` zawierał 84 rekordy z datami spoza stycznia 2023
- Konsekwencja: Bez filtrowania ładowanie przyrostowe (`--mode incremental`) tworzyłoby duplikaty przy kolejnych uruchomieniach
- Rozwiązanie: Filtr `tpep_pickup_datetime >= month_start AND < next_month` w `taxi_ingest.py`

**Problem 2 — Duplikaty w danych źródłowych TLC:**
- Opis: 1 para identycznych wierszy (wszystkie 19 kolumn identyczne) w danych TLC
- Rozwiązanie: `dropDuplicates()` po wszystkich kolumnach w `taxi_ingest.py`

### Wyniki sprawdzeń jakości (styczeń–luty 2023, stan po naprawie)

| Kategoria | Sprawdzenie | Wynik | Wartość |
|---|---|---|---|
| Kompletność | Liczba wierszy: staging.fact_trip | PASS | 5 617 230 |
| Kompletność | Liczba wierszy: staging.fact_weather | PASS | 5 664 |
| Kompletność | Liczba wierszy: staging.zone_lookup | PASS | 265 |
| Kompletność | Liczba wierszy: dwh.dim_date | PASS | 59 |
| Kompletność | Liczba wierszy: dwh.dim_time | PASS | 1 440 |
| Kompletność | Liczba wierszy: dwh.dim_location | PASS | 265 |
| Kompletność | Liczba wierszy: dwh.dim_weather_type | PASS | 28 |
| Kompletność | Liczba wierszy: dwh.fact_trip | PASS | 5 617 092 |
| Kompletność | Liczba wierszy: dwh.fact_weather | PASS | 1 416 |
| Poprawność | fact_trip: null w kluczach/miarach | PASS | 0 |
| Poprawność | fact_trip: nie pozytywne miary | PASS | 0 |
| Integralność | fact_trip: sieroty date_key | PASS | 0 |
| Integralność | fact_trip: sieroty pu_location_key | PASS | 0 |
| Integralność | fact_weather: sieroty weather_type_key | PASS | 0 |
| Duplikaty | fact_weather: duplikaty godzin | PASS | 0 |
| Zakres | fact_weather: temperatura poza [-40, 50]°C | PASS | 0 |
| Zakres | fact_weather: ujemne opady/wiatr | PASS | 0 |
| Duplikaty | staging.fact_trip: duplikaty surowe | PASS | 0 |
| Spójność | staging trips (duration>0) = dwh.fact_trip | PASS | 0 |

Wszystkie **19/19 sprawdzeń: PASS**.

### Statystyki miar po załadowaniu (styczeń–luty 2023)

| Metryka | Wartość |
|---|---|
| Min. czas przejazdu | 1 s |
| Max. czas przejazdu | 423 217 s (~117 h, wartości skrajne dopuszczone przez źródło) |
| Zakres date_key | 20230101 – 20230228 |
| Zakres time_key | 0 – 235900 |
| Min. temperatura | -15,75°C |
| Max. temperatura | 19,83°C |
| Zakres opadów | 0 – 6,3 mm/h |
| Zakres prędkości wiatru | 0,53 – 38,85 km/h |
| Czas modulo w fact_weather | 0 (sekundy zawsze = 00 ✓) |

---

## 10. Testy funkcjonalne

Format wyników: cel / kroki / oczekiwany wynik / potwierdzenie.

---

### T1 — Warstwa ETL: inicjalizacja (init load)

**Cel:** Potwierdzenie pełnego załadowania danych.

**Kroki:**
1. `docker compose up -d` (start stosu, aplikacja schematu przez db-init)
2. `docker compose exec etl-runner uv run python main.py --year 2023 --start-month 1 --end-month 1 --mode init`

**Oczekiwany wynik:** Staging i DWH wypełnione, brak błędów, liczba wierszy > 0.

**Potwierdzenie (log z wykonania):**
```json
{"ts": "2026-06-11T09:10:24", "level": "INFO", "msg": "Zone lookup ingestion complete", "written": 265}
{"ts": "2026-06-11T09:14:11", "level": "INFO", "msg": "TLC ingestion complete", "year": 2023, "month": 1, "written": 2884460}
{"ts": "2026-06-11T09:14:14", "level": "INFO", "msg": "Weather ingestion complete", "year": 2023, "month": 1, "written": 2976}
{"ts": "2026-06-11T09:17:11", "level": "INFO", "msg": "Pipeline finished", "fact_trip_total": 2884355, "fact_weather_total": 744}
{"ts": "2026-06-11T09:17:12", "level": "INFO", "msg": "Load finished", "year": 2023, "months": [1], "mode": "init"}
```

---

### T2 — Warstwa ETL: kolejna iteracja (incremental load, idempotentność)

**Cel:** Potwierdzenie, że ponowne załadowanie tego samego okresu nie tworzy duplikatów.

**Kroki:**
1. Po wykonaniu T1: `docker compose exec etl-runner uv run python main.py --year 2023 --start-month 1 --end-month 1 --mode incremental`
2. Porównanie liczby wierszy przed i po

**Oczekiwany wynik:** Identyczna liczba wierszy co po T1 (2 884 355 dla fact_trip, 744 dla fact_weather).

**Potwierdzenie (log z wykonania):**
```json
{"ts": "2026-06-11T09:21:40", "level": "INFO", "msg": "Pipeline finished", "fact_trip_total": 2884355, "fact_weather_total": 744}
```
Wynik identyczny z T1 — zero duplikacji.

---

### T3 — Warstwa hurtowni: integralność schematu (klucze obce)

**Cel:** Zero naruszeń kluczy obcych w obu tabelach faktów.

**Kroki:**
1. `docker compose exec etl-runner uv run python -m src.quality.checks`
2. Sprawdzenie wyników dla kategorii: `orphaned date_key`, `orphaned pickup location`, `orphaned weather type`

**Oczekiwany wynik:** Wszystkie 3 sprawdzenia = 0 naruszeń.

**Potwierdzenie (quality_report.md):**
```
| fact_trip: orphaned date_key       | PASS | 0 | 0 |
| fact_trip: orphaned pickup location| PASS | 0 | 0 |
| fact_weather: orphaned weather type| PASS | 0 | 0 |
```

---

### T4 — Warstwa hurtowni: poprawność transformacji

**Cel:** Weryfikacja poprawności transformacji kluczy i miar.

**Kroki:**
1. SQL: `SELECT MIN(trip_duration_sec), MIN(date_key), MAX(date_key) FROM dwh.fact_trip;`
2. SQL: `SELECT COUNT(*) FROM dwh.fact_weather WHERE (time_key % 100) != 0;`
3. SQL: `SELECT COUNT(*) FROM dwh.fact_trip WHERE trip_duration_sec <= 0;`

**Oczekiwany wynik:**
- `trip_duration_sec` > 0 dla wszystkich wierszy
- `date_key` w zakresie 20230101–20230131 (tylko styczeń)
- Sekundy `time_key` zawsze = 00 (time_key mod 100 = 0)

**Potwierdzenie (wyniki SQL):**
```
MIN(trip_duration_sec): 1
MIN(date_key): 20230101, MAX(date_key): 20230131
COUNT(*) WHERE time_key % 100 != 0: 0
COUNT(*) WHERE trip_duration_sec <= 0: 0
```
```
| fact_trip: non-positive measures | PASS | 0 | 0 |
```

---

### T5 — Spójność danych: staging ↔ DWH

**Cel:** Każdy oczyszczony rekord ze staging trafia do `dwh.fact_trip` (bez gubienia danych w transformacji).

**Kroki:**
1. `python -m src.quality.checks` → sprawdzenie `consistency: staging trips (duration > 0) = dwh.fact_trip`

**Oczekiwany wynik:** Różnica = 0 (liczba rekordów staging z `trip_duration > 0` = liczba wierszy `dwh.fact_trip`).

**Potwierdzenie:**
```
| consistency: staging trips (duration > 0) = dwh.fact_trip | PASS | 0 | 0 (staging=2884355, dwh=2884355) |
```

---

### T6 — Test end-to-end (całościowe działanie systemu)

**Cel:** Potwierdzenie poprawnego działania całego systemu od pobrania danych źródłowych do gotowości danych w hurtowni.

**Kroki:**
1. `docker compose down -v && docker compose up -d` (świeży start, aplikacja schematu)
2. `docker compose exec etl-runner uv run python main.py --year 2023 --start-month 1 --end-month 1 --mode init`
3. `docker compose exec etl-runner uv run python -m src.quality.checks`
4. SQL: `SELECT d.full_date, wt.condition_name, COUNT(t.*) FROM dwh.fact_trip t JOIN dwh.dim_date d ON t.date_key=d.date_key JOIN dwh.fact_weather w ON t.date_key=w.date_key AND (t.time_key/10000)=(w.time_key/10000) JOIN dwh.dim_weather_type wt ON w.weather_type_key=wt.weather_type_key GROUP BY 1,2 LIMIT 5;`

**Oczekiwany wynik:** Schemat zastosowany bez błędów, ETL kończy bez wyjątków, 19/19 sprawdzeń jakości PASS, zapytanie analityczne zwraca wiersze.

**Potwierdzenie (log z wykonania):**
```json
{"ts": "2026-06-11T09:31:54", "level": "INFO", "msg": "Quality checks finished", "total": 19, "failed": 0}
ALL_GREEN
```
Schema init: `INSERT 0 1440` (dim_time), `INSERT 0 28` (dim_weather_type), `0 errors`.

---

## 11. Podsumowanie

### Ocena techniczna
 Zaimplementowano:
- **6 tabel** w schemacie `dwh` (4 wymiary + 2 fakty) z rzeczywistymi kluczami obcymi
- **3 moduły ingresji** (TLC, Open-Meteo, Zone Lookup) z obsługą błędów i mechanizmem ponowień
- **Dwa scenariusze ładowania** (init i incremental) — oba idempotentne
- **19 automatycznych sprawdzeń jakości** uruchamianych po każdym załadowaniu
- **Konteneryzację** (Docker Compose) zapewniającą identyczność środowiska

### Ocena biznesowa

| Kryterium | Ocena |
|---|---|
| Poprawność danych | Wszystkie 19 kontroli jakości PASS; exact match staging ↔ DWH |
| Skalowalność | PySpark umożliwia przetwarzanie wielu miesięcy równolegle |
| Niezawodność | Idempotentne ładowanie — bezpieczny re-run i dołączanie nowych miesięcy |
| Dostępność dla BI | Tableau łączy się bezpośrednio; model gotowy dla wszystkich 6 raportów |
| Czas ładowania | Styczeń 2023 (2,88 mln przejazdów): ~8 min na single-node Spark |

System umożliwia realizację wszystkich 6 raportów biznesowych zdefiniowanych w raporcie wstępnym. Model danych jest gotowy do podłączenia Tableau i tworzenia interaktywnych dashboardów.

---

## 12. Podział pracy w zespole

| Zadanie | Wykonawca |
|---|---|
| Projekt modelu DWH | Oleksii Vinichenko |
| Implementacja ETL | Maksim Razantsau |
| Pipeline transformacji| Maksim Razantsau |
| Moduł jakości danych (quality/checks.py) | Oleksii Vinichenko |
| Konfiguracja Docker Compose | Maksim Razantsau |
| Testowanie i weryfikacja end-to-end | Maksim Razantsau |
| [do uzupełnienia] | Oleksii Vinichenko |
