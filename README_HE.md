<div dir="rtl" lang="he" align="right">
<h1 dir="rtl" align="right">Kafka בפייתון — שלוש מחברות ללימוד ולתרגול</h1>
<p dir="rtl" align="right">סביבת העבודה היא <strong>Cursor, uv ו־Python 3.13.12</strong>. מתחילים בעשרה אירועים קבועים ללימוד היסודות, וממשיכים לקליטה רציפה מ־Wikimedia דרך Kafka אל OpenSearch.</p>
<p dir="rtl" align="right"><strong>מדריכים:</strong> <a href="README.md">ה־README הראשי</a> כולל את מסלול ההתקנה וההרצה; <a href="docs/NOTEBOOK_WALKTHROUGH_HE.md">המדריך המפורט לשתי המחברות</a> מסביר את המושגים, התרשימים, התוצאות והמונים. לפתיחה מקומית בדפדפן השתמשו ב־<a href="START_HERE_HE.html">START_HERE_HE.html</a> או ב־<a href="Kafka_Installation_Guide_HE.html">מדריך ההתקנה</a>.</p>
<table dir="rtl" width="100%">
<thead><tr><th dir="rtl" align="right">סדר הלימוד</th><th dir="rtl" align="right">מה עושים?</th><th dir="rtl" align="right">מתי מסתיים?</th></tr></thead>
<tbody>
<tr><td dir="rtl" align="right"><a href="01_Kafka_Synthetic_HE.ipynb">01 — עשרה אירועים סינתטיים</a></td><td dir="rtl" align="right">שולחים עשרה אירועים ל־Kafka, קוראים אותם, מבצעים Replay ומחשבים ספירות וחלונות זמן.</td><td dir="rtl" align="right">הרצה סופית; מתאים ל־Run All.</td></tr>
<tr><td dir="rtl" align="right"><a href="02_Wikimedia_Streaming_OpenSearch_HE.ipynb">02 — Wikimedia Streaming אל OpenSearch</a></td><td dir="rtl" align="right">קולטים אירועים מהאינטרנט ברצף; Producer שולח ל־Kafka ו־Consumer כותב ל־OpenSearch.</td><td dir="rtl" align="right">ללא תקרת כמות או זמן כוללת; עוצרים את תא הזרם ידנית.</td></tr>
<tr><td dir="rtl" align="right"><a href="03_Wikimedia_Confluent_Cloud_HE.ipynb"><strong>3. Streaming דרך Confluent Cloud</strong></a></td><td dir="rtl" align="right">אותו מקור Wikimedia ואותו יעד OpenSearch, עם Kafka מנוהל בענן וחיבור SASL/TLS.</td><td dir="rtl" align="right">ללא מגבלת כמות או זמן כוללת; עוצרים ידנית. נדרשים חשבון ענן ו־Kafka API Key.</td></tr>
</tbody></table>
<h2 dir="rtl" align="right">1. מכינים את הפרויקט</h2>
<p dir="rtl" align="right">התקינו Git, Cursor, uv ו־Docker Desktop לפי מדריך ההתקנה. ב־Cursor התקינו הרחבות Python ו־Jupyter ופתחו את <strong>כל תיקיית הפרויקט</strong>, שבה נמצאים <code dir="ltr">pyproject.toml</code>, <code dir="ltr">uv.lock</code> והתיקייה <code dir="ltr">scripts</code>. במסוף מתוך התיקייה הריצו:</p>
</div>

```powershell
uv python install 3.13.12
uv sync --locked
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">בחרו בכל מחברת <strong>Select Kernel → Python Environments → .venv</strong>. ב־Windows הנתיב הוא <code dir="ltr">.venv\Scripts\python.exe</code>; ב־macOS או Linux הוא <code dir="ltr">.venv/bin/python</code>. תא הפתיחה מדפיס את גרסת Python ואת הנתיב בפועל. את פקודות המסוף אין להדביק בתא Python.</p>
<h2 dir="rtl" align="right">2. מפעילים Kafka ומריצים את מחברת 01</h2>
<p dir="rtl" align="right">פתחו Docker Desktop, המתינו להפעלת המנוע במצב Linux containers, ואז במסוף הפרויקט:</p>
</div>

```powershell
uv run --locked python scripts/kafka_docker.py start
uv run --locked python scripts/check_kafka.py
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">פתחו את <a href="01_Kafka_Synthetic_HE.ipynb">מחברת 01</a>, השאירו <code dir="ltr">USE_KAFKA = True</code> והפעילו <strong>Restart Kernel and Run All</strong>. הנתונים סינתטיים וקבועים, אך ברירת המחדל מעבירה אותם דרך Kafka אמיתי. צפויים עשרה אירועים, שישה בוטים וארבעה שאינם בוטים. Replay קורא את אותן רשומות בקבוצה חדשה, בלי לשלוח אירועים נוספים.</p>
<p dir="rtl" align="right">לתרגול ללא שרת אפשר לבחור במפורש <code dir="ltr">USE_KAFKA = False</code>. הפלט יסומן OFFLINE ולא תתבצע תקשורת אמיתית עם Kafka.</p>
<h2 dir="rtl" align="right">3. מפעילים OpenSearch ומריצים את מחברת 02</h2>
<p dir="rtl" align="right">הכינו זיכרון והגדרות WSL לפי <a href="Kafka_Installation_Guide_HE.html">סעיפי OpenSearch במדריך ההתקנה</a>. לאחר מכן:</p>
</div>

```powershell
uv run --locked python scripts/opensearch_docker.py start
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">פתחו את <a href="02_Wikimedia_Streaming_OpenSearch_HE.ipynb">מחברת 02</a> והתקדמו לפי הסדר: הגדרות, בדיקת חיבורים ותא הזרם. <code dir="ltr">MAX_EVENTS = None</code> ו־<code dir="ltr">MAX_SECONDS = None</code> כבר מוגדרים, ולכן אין תקרה כוללת לקליטה. ה־Topic, קבוצת הצרכנים וה־Index נשארים קבועים בין הרצות.</p>
<p dir="rtl" align="right"><strong>תא הזרם נשאר פעיל בכוונה.</strong> עצרו באמצעות כפתור העצירה של התא והמתינו לסגירה. אם התחלתם ב־Run All, בטלו את ההמשך לפי הצורך ואז הריצו ידנית את תאי הדוח, החיפוש והסיכום. אין צורך לאתחל את ה־Kernel כדי לעצור.</p>
<p dir="rtl" align="right">הצרכן מקבל אישור כתיבה מ־OpenSearch לפני Commit ב־Kafka. מזהה מסמך קבוע מאפשר כתיבה חוזרת לאותו אירוע בלי מסמך נוסף, אך אין כאן עסקה אטומית בין שני השרתים. המונים יכולים לכלול backlog מהרצות קודמות; מספר פעולות כתיבה אינו בהכרח מספר מסמכים ייחודיים. פירוט מלא נמצא במדריך שתי המחברות.</p>
<h2 dir="rtl" align="right">4. חלופת ענן למחברת 02</h2>
<p dir="rtl" align="right"><a href="03_Wikimedia_Confluent_Cloud_HE.ipynb">מחברת 03</a> מחברת את אותו זרם Wikimedia ל־Kafka ב־Confluent Cloud. Python ו־OpenSearch נשארים במחשב. הכינו Topic, הרשאות ו־Kafka API Key לפי <a href="docs/CONFLUENT_CLOUD_HE.md">מדריך הענן</a>, ושמרו את ההגדרות בקובץ <code dir="ltr">confluent-cloud.properties</code> על בסיס קובץ הדוגמה. אין צורך ב־Kafka מקומי במסלול הזה.</p>
<p dir="rtl" align="right">גם כאן הזרימה רציפה עד עצירה. שמרו על סודיות המפתח, ובדקו את חיובי משאבי הענן בסיום; עצירת התא אינה מוחקת אותם.</p>
<h2 dir="rtl" align="right">5. מסיימים את העבודה המקומית</h2>
<p dir="rtl" align="right">לאחר שתא הזרם נסגר, עצרו את השירותים:</p>
</div>

```powershell
uv run --locked python scripts/opensearch_docker.py stop
uv run --locked python scripts/kafka_docker.py stop
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">פקודות העצירה משאירות את המכולות במקומן, ונתוני OpenSearch נשמרים ב־volume. בהפעלה הבאה משתמשים שוב בפקודות start. לעזרה בחיבור Kafka ראו את <a href="docs/KAFKA_TROUBLESHOOTING_HE.md">מדריך התקלות</a>; שם ה־Topic במחברות החדשות נמצא במשתנה <code dir="ltr">TOPIC</code>.</p>
</div>
