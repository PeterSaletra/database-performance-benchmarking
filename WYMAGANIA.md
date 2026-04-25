 Ocena 3.0 – poziom podstawowy
Aby uzyskać ocenę 3.0, projekt musi zawierać:
1. Cel i zakres pracy – jasno określony temat oraz zakres analiz.
2. Opis wybranych systemów zarządzania bazami danych (SZBD).
3. Zalety i wady wybranych baz danych, w tym udogodnienia oraz ograniczenia.
4. Awaryjność, bezpieczeństwo, migracje, integracje i skalowalność – część
teoretyczna.
5. Obszary biznesowych zastosowań wybranych systemów zarządzania bazami danych.
6. Opis zbioru danych – co najmniej 5 tabel w systemie relacyjnym.
7. Krótki opis aplikacji testowej, obejmujący:
o zdefiniowanie wymagań,
o wykorzystane technologie i narzędzia,
o opis działania aplikacji.
8. Opis przeprowadzonych testów wydajnościowych oraz porównanie operacji CRUD
dla:
o małego,
o średniego,
o dużego zbioru danych
(np. 10 000, 100 000, 1 000 000 rekordów).
9. Porównanie co najmniej 4 systemów baz danych:
o 2 systemów relacyjnych,
o 2 systemów nierelacyjnych.
10. Co najmniej 12 scenariuszy testowych,
w tym minimum 3 scenariusze dla każdej operacji CRUD.
11. Średnią z 3 prób dla każdej operacji CRUD
(minimum 3 próby dla każdego z 12 scenariuszy testowych).
12. Opracowanie wyników testów w formie:
o opisu,
o wizualizacji (np. wykresów),
przedstawionych jako sprawozdanie oraz prezentacja.
 Ocena 4.0 – poziom rozszerzony
Aby uzyskać ocenę 4.0, projekt musi spełniać wymagania poziomu 3.0 oraz dodatkowo
zawierać:
1. Co najmniej dwa różne modele danych, np.:
o relacyjny,
o dokumentowy,
o grafowy
(w zależności od wybranego silnika bazodanowego).
2. Wykorzystanie indeksów w bazach danych.
3. Co najmniej 10 tabel w systemie relacyjnym.
4. Analizę planów zapytań (np. z wykorzystaniem EXPLAIN).
5. Porównanie wyników testów przed i po zastosowaniu indeksów.
6. Średnią z 3 prób dla każdej operacji CRUD
(minimum 3 próby dla każdego z 24 scenariuszy testowych).
7. Rozszerzoną analizę wyników oraz wniosków.
8. Co najmniej 24 różne scenariusze testowe,
w tym minimum 6 scenariuszy dla każdej operacji CRUD.
9. Opis testów wydajnościowych oraz porównanie operacji CRUD dla:
o małego,
o średniego,
o dużego zbioru danych
(np. 500 000, 1 000 000, 10 000 000 rekordów).
 Ocena 5.0 – poziom zaawansowany
Aby uzyskać ocenę 5.0, projekt musi spełniać wymagania poziomu 4.0 oraz zawierać
elementy zaawansowane.
Wymagane jest:
• wykonanie co najmniej dwóch z poniższych punktów,
•Testy skalowalności (równoległe zapytania, wielu użytkowników).
•Analiza bezpieczeństwa (role, uprawnienia, szyfrowanie) oraz ich wpływ na
operacje CRUD.
•Wykorzystanie danych półustrukturalnych (np. JSON, dokumenty) i ich
przechowywanie w wybranym silniku bazodanowym.
•Automatyzacja testów oraz generowania wyników.
• sformułowanie i weryfikacja jednej hipotezy badawczej.
Hipoteza badawcza
Należy opracować jedną hipotezę (własną lub zaproponowaną przez prowadzącego).
Przykładowe hipotezy badawcze:
H1: Indeksy a wydajność
Zastosowanie indeksów znacząco poprawia wydajność operacji SELECT kosztem spadku
wydajności operacji INSERT i UPDATE.
H2: Wpływ rozmiaru danych
Różnice wydajności pomiędzy silnikami bazodanowymi rosną wraz ze wzrostem liczby
rekordów.
H3: Normalizacja vs denormalizacja
Denormalizacja danych poprawia wydajność zapytań odczytowych kosztem operacji
modyfikujących dane.