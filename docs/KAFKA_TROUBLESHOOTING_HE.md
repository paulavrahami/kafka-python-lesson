<div dir="rtl" lang="he">

# אבחון Message timed out בחיבור ל־Kafka

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
| ה־bootstrap עובד אך כתובת Broker נכשלת | בדקו את `advertised.listeners` ואת סביבת ה־Kernel. `localhost` מתוך Windows, WSL או מכולה עשוי להתייחס לסביבה שונה. |
| ה־Topic אינו מופיע | ודאו שהעתקתם את השם מה־Kernel הנוכחי. תא ההגדרות יוצר `RUN_ID` חדש; תא יצירת ה־Topics והשליחה צריכים להתייחס לאותו ניסוי. |
| `leader=-1` או Leader שאינו מופיע ב־metadata | המחיצה אינה מוכנה. בדקו את לוגי השרת. |
| ה־Leader חסר ב־ISR | המחיצה אינה מציגה Leader מסונכרן; בדקו את מצב ה־Broker. |
| כל בדיקות metadata מצליחות אך השליחה נכשלת | שמרו את הודעת השגיאה המלאה, במיוחד `broker_errors`, ואת לוגי Docker. צריך לבדוק את מסלול השליחה, הרשאות הכתיבה ומצב השרת בזמן הכשל. |

התמונה הרשמית בתצורת המעבדה המקומית משתמשת ב־`localhost:9092`; זו כתובת תקינה כאשר ה־Kernel פועל באותו מחשב. אין להסיק מה־timeout לבדו שצריך להחליף כתובות או ליצור מחדש את המכולה.

## 3. מה השתנה במחברת

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

## מקורות

- [Confluent Python API: callbacks, metadata ו־flush](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka: delivery timeouts ומשמעות אישורי המסירה](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html)
- [librdkafka: הגדרות זמני המתנה](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [Apache Kafka: Docker](https://kafka.apache.org/44/getting-started/docker/)

</div>
