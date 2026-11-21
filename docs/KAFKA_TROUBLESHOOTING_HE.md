<div dir="rtl" lang="he">

# אבחון תקלות חיבור ומסירה ב־Kafka

השגיאה `KafkaError{code=_MSG_TIMED_OUT}` אומרת שה־Producer לא קיבל אישור מסירה מוצלח בזמן שהוגדר. גם הרצה מסודרת באמצעות **Run All** יכולה להיכשל בגלל זמינות השרת, חיבור רשת, כתובת שהשרת מפרסם ללקוחות, מחיצה שעדיין אינה מוכנה או עיכוב בתשובה.

`remaining: 0` מציין שלא נשארו הודעות בתור המקומי. הוא אינו מוכיח הצלחה: הודעות שנכשלו יוצאות גם הן מהתור. אם לא התקבל אישור, ייתכן שחלק מההודעות בכל זאת נכתבו בשרת. אל תריצו שוב ושוב את תא השליחה לאותו Topic לפני בדיקת המצב; Producer חדש אינו מבטל כפילויות עסקיות משליחות קודמות.

## 1. בדיקת Docker והחיבור מהמחשב

פתחו Docker Desktop. מתוך תיקיית הפרויקט, במסוף PowerShell של Cursor, הפעילו את הפקודות אחת אחרי השנייה:

</div>

```powershell
uv run --locked python scripts/kafka_docker.py start
uv run --locked python scripts/check_kafka.py
uv run --locked python scripts/kafka_docker.py logs
```

<div dir="rtl" lang="he">

`start` ממתינה שממשק Kafka בתוך המכולה ישיב. `check_kafka.py` בודקת גם שהמחשב מצליח להגיע לכתובת ההתחלתית וגם לכתובות שהשרת מפרסם ב־metadata. `logs` מציגה את 80 שורות הלוג האחרונות.

אם בדיקת החיבור נכשלת, העתיקו את הפלט המלא שלה. אם היא מצליחה, המשיכו לבדיקה של ה־Topic המסוים.

### החיבור ל־127.0.0.1 הצליח, אך localhost נכשל

שגיאה עם `stage: advertised_broker_connection`, כתובת התחלתית `127.0.0.1:9092` ו־Broker בשם `localhost:9092` מצביעה על כשל בבדיקת metadata נפרדת מול הכתובת שהשרת מפרסם. היא אינה מוכיחה שהמכולה קרסה או שהתמונה אינה יציבה.

`localhost` עשוי להיפתר גם ל־IPv4 וגם ל־IPv6, בעוד שסקריפט Docker של המעבדה מפרסם את הפורט על `127.0.0.1` בלבד. לכן כל לקוחות המחברת — AdminClient, Producer ו־Consumer — וסקריפט האבחון משתמשים כעת ב־`broker.address.family="v4"`. כך גם כתובת `localhost` תיבדק דרך IPv4. זו התאמה להגדרת המעבדה; הפלט לבדו אינו מוכיח ש־IPv6 היה הגורם לכשל.

לקריאת metadata שנכשלת בשגיאת רשת זמנית ניתן ניסיון נוסף אחד, לאחר רבע שנייה, באותו AdminClient. לכל בקשה מגבלה של 10 שניות כברירת מחדל; כל כתובת נבדקת בעד שתי בקשות. זמן הקמת חיבור מוגבל בנפרד ל־5 שניות. אין להסיק מכך שכל תהליך יצירת ה־Topics מוגבל ל־10 שניות.

סקריפט האבחון מדפיס את הכתובת **לפני** בדיקתה ואת משפחת הכתובות שנבחרה. אם הכשל ממשיך לאחר העדכון, שמרו את הפלט ולוגי Docker. למשתמשים שבודקים תצורה אחרת קיימת אפשרות `--address-family any` או `--address-family v6`; במעבדה המקומית משאירים את ברירת המחדל `v4`.

ליישום תיקון החיבור מעדכנים את הפרויקט ומאתחלים את ה־Kernel לפי סעיף 4. אין צורך למחוק את המכולה, להחליף Image או להשבית IPv6 במערכת ההפעלה כדי להחיל אותו.

אם השגיאה מופיעה בפרסום `timeseries` בשלב האחרון, ייתכן שפרסומי `bots` ו־`websites` הקודמים כבר הושלמו. ההודעה שלא הוכנסו רשומות לתור מתייחסת לאצווה שנכשלה בבדיקה, ולא לכל התאים או לכל האצוות שכבר רצו.

## 2. בדיקת אותו Topic שאליו המחברת ניסתה לשלוח

**במחברת**, בתא Python חדש, הריצו:

</div>

```python
import sys
print("Kernel:", sys.executable)
print("Bootstrap:", BOOTSTRAP_SERVERS)
print("Wiki topic:", WIKI_TOPIC)
```

<div dir="rtl" lang="he">

העתיקו את שם ה־Topic מהפלט. **במסוף PowerShell**, החליפו את הטקסט בין המרכאות בשם שהודפס:

</div>

```powershell
uv run --locked python scripts/check_kafka.py --topic "PASTE_THE_WIKI_TOPIC_FROM_THE_NOTEBOOK"
```

<div dir="rtl" lang="he">

הבדיקה מציגה את המחיצות, ה־Leader, העותקים וה־ISR — קבוצת העותקים המסונכרנים. היא משתמשת ב־metadata הקיים ואינה יוצרת Topic חסר. הצלחת בדיקת metadata אינה עדיין הוכחה לשליחה וצריכה מוצלחות.

| מה מופיע | המשמעות והצעד הבא |
|---|---|
| החיבור ל־bootstrap נכשל | בדקו ש־Docker פועל ושהכתובת היא `127.0.0.1:9092` ב־Kernel מקומי באותו מחשב. |
| ה־bootstrap עובד אך כתובת Broker נכשלת | ודאו שהקוד המעודכן משתמש ב־`v4`; ראו את ההסבר על `localhost` לעיל. בדקו גם את `advertised.listeners` ואת סביבת ה־Kernel: Windows, WSL ומכולות עשויים להשתמש בכתובת לולאה מקומית שונה. |
| ה־Topic אינו מופיע | ודאו שהעתקתם את השם מה־Kernel הנוכחי. תא ההגדרות יוצר `RUN_ID` חדש; תא יצירת ה־Topics והשליחה צריכים להתייחס לאותו ניסוי. |
| `leader=-1` או Leader שאינו מופיע ב־metadata | המחיצה אינה מוכנה. בדקו את לוגי השרת. |
| ה־Leader חסר ב־ISR | המחיצה אינה מציגה Leader מסונכרן; בדקו את מצב ה־Broker. |
| כל בדיקות metadata מצליחות אך השליחה נכשלת | שמרו את הודעת השגיאה המלאה, במיוחד `broker_errors`, ואת לוגי Docker. צריך לבדוק את מסלול השליחה, הרשאות הכתיבה ומצב השרת בזמן הכשל. |

התמונה הרשמית בתצורת המעבדה המקומית משתמשת ב־`localhost:9092`; זו כתובת תקינה כאשר ה־Kernel פועל באותו מחשב. אין להסיק מה־timeout לבדו שצריך להחליף כתובות או ליצור מחדש את המכולה.

## 3. מה השתנה במחברת

- לכל לקוחות Kafka הגדרת IPv4 משותפת, התואמת למיפוי הפורט המקומי של Docker. בבדיקות החיבור יש ניסיון נוסף אחד רק לקריאת metadata שנכשלה בשגיאה זמנית, ללא שליחה חוזרת אוטומטית של אצוות נתונים.
- לפני השליחה נבדקות כתובת ה־bootstrap וכתובות ה־Brokers מה־metadata, מתוך אותו Kernel שמריץ את השיעור.
- לאחר יצירת ה־Topic ממתינים ל־metadata של המחיצות, ל־Leader זמין ולנוכחותו ב־ISR. זו המתנה נפרדת עם מגבלה של 30 שניות; בדיקות החיבור ויצירת ה־Topics מוגבלות בנפרד.
- לפני כל אצווה נבדק מחדש ה־Topic שאליו נשלחת אותה אצווה.
- זמן המסירה הוגדל מ־15 ל־30 שניות; `flush` מוגבלת ל־35 שניות כדי להשלים את ה־Callbacks.
- שגיאת השליחה כוללת `stage`, כתובת חיבור, Topic, מספר הודעות שהתקבלו לתור, מספר אישורים, שגיאות מסירה ושגיאות Broker.

בדיקות אלה מתקנות חוסר בבדיקת המוכנות ומשפרות את האבחון. הן אינן מבטיחות שהשרת יישאר זמין לאחר הבדיקה, ואינן קובעות מה גרם לכשל במחשב שלכם.

## 4. עדכון הפרויקט והרצת הקוד החדש

סגרו את ה־Kernel. אם לא שיניתם קבצים מקומיים, אפשר לעדכן במסוף:

</div>

```powershell
git pull --ff-only
uv sync --locked
```

<div dir="rtl" lang="he">

אם Git מדווח על שינויים מקומיים במחברת, הורידו עותק חדש לתיקייה אחרת כדי לקבל את העדכון ולשמור את העבודה הקודמת:

</div>

```powershell
git clone https://github.com/paulavrahami/kafka-python-lesson.git kafka-python-lesson-updated
```

<div dir="rtl" lang="he">

פתחו את התיקייה החדשה ב־Cursor, הריצו בה `uv sync --locked` ובחרו את ה־Kernel שלה. הגדירו שוב `RUN_LIVE_KAFKA=True`, השאירו את Wikimedia ו־OpenSearch כבויים, והפעילו **Restart Kernel → Run All**. אתחול ה־Kernel חשוב כדי שהגדרות הפונקציות בזיכרון יתעדכנו.

## האם כדאי להחליף את תמונת Docker?

הפרויקט משתמש בתמונת ה־JVM הרשמית `apache/kafka:4.4.0`, עם גרסה מפורשת. החלפה לתמונה אחרת אינה כשלעצמה תיקון לכתובת שה־Broker מפרסם או לבחירת IPv4/IPv6. כדי לייחס כשל לתמונה צריך גם לוגי שרת או קריסה שחוזרת בתצורה תקינה. `confluentinc/cp-kafka` היא חלופה רשמית של Confluent, אך אין כאן בדיקה שמוכיחה שהיא יציבה יותר בתנאי המעבדה. תמונת `apache/kafka-native` מוגדרת בתיעוד Apache כניסיונית.

**עדכון על סביבת Java, 21 בנובמבר 2026:** Apache פרסמה ב־19 בנובמבר הודעה על CVE-2026-114938 ב־JRE המצורף לתמונות JVM, ובהן `4.4.0`. לפי ההודעה, הפגיעות חלה בשילוב גרסת Java מושפעת ומעבד x86-64 עם AVX-512 VBMI. ההמלצה היא JRE מתוקן; עד אז מתועדת אפשרות ההפעלה `KAFKA_OPTS="-XX:-UseBASE64Intrinsics"`. אם קיימות אפשרויות Java, מוסיפים את הדגל לערכן הקיים. ראו את [ההנחיה הרשמית של Apache](https://kafka.apache.org/blog/2026/11/19/mitigating-cve-2026-114938-java4shell-in-apache-kafka-deployments/) ואת [עדכון Adoptium](https://adoptium.net/news/2026/11/temurin-november-2026-cspu-java4shell).

זו הנחיה נפרדת מתיקון חיבור ה־metadata. תיקון המחברת אינו משנה את הגדרות Java במכולה קיימת. שינוי משתנה סביבה מחוץ למכולה או עדכון הקבצים ב־Git אינם מעדכנים את סביבת תהליך Kafka שכבר פועל; שינוי ההפעלה דורש טיפול נפרד ששומר על הנתונים הקיימים.

## מקורות

- [Confluent Python API: callbacks, metadata ו־flush](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka: delivery timeouts ומשמעות אישורי המסירה](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html)
- [librdkafka: הגדרות זמני המתנה](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [librdkafka FAQ: חיבורי localhost ו־IPv4/IPv6](https://github.com/confluentinc/librdkafka/wiki/FAQ#connecting-to-ipv6-addresses-connect-to-ipv61-9092-failed-connection-refused)
- [Apache Kafka: Docker](https://kafka.apache.org/44/getting-started/docker/)
- [Confluent: תמונות Docker רשמיות](https://docs.confluent.io/platform/current/installation/docker/image-reference.html)

</div>
