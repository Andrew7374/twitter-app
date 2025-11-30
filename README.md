# Twitter Data Analysis with PySpark

## 📖 Opis projektu
Projekt powstał podczas przerabiania kursu **Fundament Apache Spark** od Akademii BigData.  
Jego celem było sfinalizowanie zdobytej wiedzy i praktyczne wykorzystanie PySpark do analizy danych.

Aplikacja:
- ładuje dane dotyczące tweetów z plików **CSV**,
- oczyszcza dane (m.in. normalizacja hashtagów, usuwanie duplikatów),
- wykonuje podstawowe statystyki i agregacje (np. liczba tweetów z danego źródła, popularność hashtagów).

---

## 🛠️ Użyte technologie
- **Python** – język programowania
- **PySpark** – silnik do rozproszonego przetwarzania danych
- **CSV** – format wejściowy danych

---

## 📂 Struktura projektu

```text
TwitterApp/
│
├── analyzers/          # moduł do analizy danych
├── cleaners/           # moduł do czyszczenia danych
├── loaders/            # moduł do ładowania danych
├── twitter_app.py      # główny plik uruchomieniowy
└── README.md           # dokumentacja projektu
```

## Cel Projektu
Projekt miał charakter edukacyjny i służył jako praktyczne ćwiczenie z:
- ładowania danych w PySpark,
- czyszczenia i transformacji danych,
- wykonywania podstawowych analiz i agregacji.
