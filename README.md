<div dir="rtl" lang="he" align="right">
<h1 dir="rtl" align="right">Kafka עם Python — שתי מחברות, שני פרויקטים</h1>
<p dir="rtl" align="right">מעבדה לסטודנטים בעברית, בסביבת <strong>Cursor + uv + Python 3.13.12</strong>. מתחילים בעשרה אירועים מוכרים כדי להבין Kafka, ואז מפעילים זרם אמיתי מ־Wikimedia ושומרים אותו ב־OpenSearch עד עצירה יזומה.</p>
<table dir="rtl" width="100%">
<thead><tr>
<th dir="rtl" align="right">סדר הלימוד</th>
<th dir="rtl" align="right">מה עושים?</th>
<th dir="rtl" align="right">איך מסתיימת ההרצה?</th>
</tr></thead><tbody>
<tr><td dir="rtl" align="right"><a href="01_Kafka_Synthetic_HE.ipynb"><strong>1. עשרה אירועים סינתטיים</strong></a></td><td dir="rtl" align="right">שולחים עשרה אירועים ל־Kafka מקומי, קוראים אותם, מדגימים Replay ומחשבים ספירות וחלונות.</td><td dir="rtl" align="right">הרצה סופית. אפשר להפעיל Run All.</td></tr>
<tr><td dir="rtl" align="right"><a href="02_Wikimedia_Streaming_OpenSearch_HE.ipynb"><strong>2. Wikimedia Streaming אל OpenSearch</strong></a></td><td dir="rtl" align="right">קולטים אירועים ברצף; Producer שולח ל־Kafka ו־Consumer כותב למסמכי OpenSearch.</td><td dir="rtl" align="right">אין מגבלת כמות או זמן כוללת. עוצרים את תא הזרם ידנית.</td></tr>
</tbody></table>
<p dir="rtl" align="right"><strong>להסבר צעד אחר צעד:</strong> <a href="docs/NOTEBOOK_WALKTHROUGH_HE.md">המדריך המפורט לשתי המחברות</a>. הוא כולל שני תרשימים, מושגים, תוצאות צפויות ופירוש המונים.</p>
<p dir="rtl" align="right"><strong>להתחלה במחשב:</strong> פתחו מקומית בדפדפן את <a href="START_HERE_HE.html">START_HERE_HE.html</a>. להתקנה מלאה ולפתרון תקלות פתחו את <a href="Kafka_Installation_Guide_HE.html">Kafka_Installation_Guide_HE.html</a>. GitHub עשוי להציג את קוד ה־HTML; את הגרסה המעוצבת פותחים מתוך תיקיית הפרויקט שהורדתם.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">מה מתקינים?</h2>
<table dir="rtl" width="100%">
<thead><tr>
<th dir="rtl" align="right">רכיב</th>
<th dir="rtl" align="right">תפקיד</th>
</tr></thead><tbody>
<tr><td dir="rtl" align="right"><a href="https://git-scm.com/downloads">Git</a></td><td dir="rtl" align="right">הורדה ועדכון של הפרויקט.</td></tr>
<tr><td dir="rtl" align="right"><a href="https://cursor.com/downloads">Cursor</a></td><td dir="rtl" align="right">עריכת קוד והרצת מחברות; התקינו בעורך את הרחבות Python ו־Jupyter.</td></tr>
<tr><td dir="rtl" align="right"><a href="https://docs.astral.sh/uv/getting-started/installation/">uv</a></td><td dir="rtl" align="right">התקנת Python 3.13.12 וניהול חבילות לפי uv.lock.</td></tr>
<tr><td dir="rtl" align="right"><a href="https://docs.docker.com/desktop/setup/install/windows-install/">Docker Desktop</a></td><td dir="rtl" align="right">הפעלת Kafka ו־OpenSearch במכולות Linux מקומיות.</td></tr>
</tbody></table>
<p dir="rtl" align="right">המסלול מניח ש־Cursor וה־Kernel רצים מקומית באותו מחשב שבו פועל Docker Desktop. אין צורך בחשבון Confluent או בשירות ענן למעבדה זו.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">1. מורידים ומכינים את סביבת Python</h2>
<p dir="rtl" align="right">במסוף PowerShell:</p>
</div>

```powershell
git clone https://github.com/paulavrahami/kafka-python-lesson.git
cd kafka-python-lesson
uv python install 3.13.12
uv sync --locked
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">ב־Cursor בחרו <strong>File → Open Folder</strong> ופתחו את כל תיקיית הפרויקט. במסוף המשולב מריצים את פקודות המדריך; אין להדביק פקודות PowerShell בתא Python.</p>
<p dir="rtl" align="right">בחרו במחברת <strong>Select Kernel → Python Environments → .venv</strong>. ב־Windows הנתיב הוא <code dir="ltr">.venv\Scripts\python.exe</code>. התא הראשון מדפיס את הגרסה ואת הנתיב. החבילות, כולל ipykernel, confluent-kafka, requests ו־opensearch-py, מנוהלות באמצעות uv.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">2. מפעילים Kafka ומריצים את פרויקט 1</h2>
<p dir="rtl" align="right">פתחו Docker Desktop מתפריט Start והמתינו להפעלת המנוע. במסוף הפרויקט:</p>
</div>

```powershell
uv run --locked python scripts/kafka_docker.py start
uv run --locked python scripts/check_kafka.py
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">הסקריפט מוריד, לפי הצורך, את <code dir="ltr">apache/kafka:4.4.0</code> ומפעיל מכולה בשם <code dir="ltr">kafka-python-lab</code>. בדיקת החיבור הנפרדת פונה לשרת בכתובת <code dir="ltr">127.0.0.1:9092</code> ובודקת גם את הכתובות שהוא מפרסם.</p>
<p dir="rtl" align="right">פתחו את <a href="01_Kafka_Synthetic_HE.ipynb">מחברת 01</a>, השאירו <code dir="ltr">USE_KAFKA = True</code> והפעילו <strong>Restart Kernel and Run All</strong>. בכל הרצה מלאה נוצר Topic חדש. התוצאות הצפויות: עשרה אירועים, שישה בוטים וארבעה שאינם בוטים; חלונות עשר השניות שאינם ריקים מכילים <code dir="ltr">[3, 2, 2, 1, 2]</code> אירועים.</p>
<p dir="rtl" align="right">לתרגול מקומי בלי שרת, אפשר לבחור במפורש <code dir="ltr">USE_KAFKA = False</code>. אז הפלט מסומן OFFLINE ואין שליחה, צריכה או Commit מול Kafka.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">3. מוסיפים OpenSearch ומריצים את פרויקט 2</h2>
<p dir="rtl" align="right">לפני ההפעלה הראשונה הכינו לפחות 4 GB זיכרון זמינים ל־Docker, בהתאם ל־<a href="https://docs.opensearch.org/latest/install-and-configure/install-opensearch/docker/">תיעוד OpenSearch</a>. Kafka ו־Cursor צריכים זיכרון נוסף. ב־WSL 2 ניהול הזיכרון נעשה בהגדרות WSL; פירוט ופקודות <code dir="ltr">vm.max_map_count=262144</code> נמצאים במדריך ההתקנה.</p>
</div>

```powershell
uv run --locked python scripts/opensearch_docker.py start
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">המכולה משתמשת ב־<code dir="ltr">opensearchproject/opensearch:3.10.0</code> ובכתובת <code dir="ltr">http://127.0.0.1:9200</code>. נתוני OpenSearch נשמרים ב־volume בשם <code dir="ltr">opensearch-python-lab-data</code>. זוהי תצורת מעבדה מקומית ללא אימות, הקשורה ל־localhost.</p>
<p dir="rtl" align="right">פתחו את <a href="02_Wikimedia_Streaming_OpenSearch_HE.ipynb">מחברת 02</a> והתקדמו לפי הסדר. תא בדיקת החיבורים מכין Topic ו־Index חסרים. ההגדרות הבאות כבר קיימות במחברת:</p>
</div>

```python
TOPIC = "python.wikimedia.continuous.v1"
GROUP_ID = "python-wikimedia-opensearch-v1"
INDEX = "wikimedia-recentchanges-v1"
MAX_EVENTS = None
MAX_SECONDS = None
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right"><strong>תא הזרם נשאר פעיל בכוונה.</strong> עוצרים באמצעות כפתור העצירה של התא; בתהליך מסוף משתמשים ב־Ctrl+C. אם בחרתם Run All, בטלו את ההמשך לאחר העצירה והריצו את תאי החיפוש והסיכום ידנית. אין צורך לאתחל את ה־Kernel כדי לעצור.</p>
<p dir="rtl" align="right">השמות הקבועים מאפשרים המשך מהמיקום ששמרה קבוצת הצרכנים. כתיבה ל־OpenSearch מאושרת לפני Commit. מזהה מסמך קבוע מאפשר לעבד שוב את אותו אירוע בלי ליצור עבורו מסמך נוסף; אין כאן עסקה אטומית או הבטחת Exactly Once בין שני השרתים.</p>
<p dir="rtl" align="right">המונים עשויים לכלול backlog מהרצות קודמות: מספר הכתיבות אינו בהכרח מספר האירועים שקלט השולח הנוכחי או מספר המסמכים הייחודיים. התורים ומטמון המזהים מוגבלים בזיכרון, אך נתוני Kafka ו־OpenSearch יכולים לגדול בדיסק.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">פקודות שימושיות</h2>
<p dir="rtl" align="right">מצב ולוגים:</p>
</div>

```powershell
uv run --locked python scripts/kafka_docker.py status
uv run --locked python scripts/kafka_docker.py logs
uv run --locked python scripts/opensearch_docker.py status
uv run --locked python scripts/opensearch_docker.py logs
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">בסיום, עצרו תחילה את תא הזרם ואז את השירותים:</p>
</div>

```powershell
uv run --locked python scripts/opensearch_docker.py stop
uv run --locked python scripts/kafka_docker.py stop
```

<div dir="rtl" lang="he" align="right">
<p dir="rtl" align="right">פקודות stop משאירות את המכולות במקומן. volume הנתונים של OpenSearch נשמר. הפעלה חוזרת באמצעות start משתמשת במשאבים הקיימים. אל תמחקו מכולות או נתונים כדי לפתור תקלה בלי לבדוק אותם.</p>
<p dir="rtl" align="right">לאבחון Kafka ראו את <a href="docs/KAFKA_TROUBLESHOOTING_HE.md">מדריך התקלות</a>; בשתי המחברות החדשות שם ה־Topic הנוכחי מופיע במשתנה <code dir="ltr">TOPIC</code>.</p>
</div>

<div dir="rtl" lang="he" align="right">
<h2 dir="rtl" align="right">מפת הפרויקט ובדיקות</h2>
<table dir="rtl" width="100%">
<thead><tr>
<th dir="rtl" align="right">קובץ</th>
<th dir="rtl" align="right">תפקיד</th>
</tr></thead><tbody>
<tr><td dir="rtl" align="right"><a href="scripts/lesson_basics.py"><code dir="ltr">scripts/lesson_basics.py</code></a></td><td dir="rtl" align="right">עשרת האירועים הקבועים, עזרי Kafka וחישובי פרויקט 1.</td></tr>
<tr><td dir="rtl" align="right"><a href="scripts/wikimedia_stream.py"><code dir="ltr">scripts/wikimedia_stream.py</code></a></td><td dir="rtl" align="right">קליטה הדרגתית, אימות אירועים, חידוש חיבור ומטמון מזהים מוגבל.</td></tr>
<tr><td dir="rtl" align="right"><a href="scripts/kafka_opensearch_streaming.py"><code dir="ltr">scripts/kafka_opensearch_streaming.py</code></a></td><td dir="rtl" align="right">השולח, הצרכן והכתיבה ל־OpenSearch בפרויקט 2.</td></tr>
<tr><td dir="rtl" align="right"><a href="tests"><code dir="ltr">tests/</code></a></td><td dir="rtl" align="right">בדיקות אוטומטיות להתנהגות, להתאוששות, לקליטה ולהכנת השירותים.</td></tr>
</tbody></table>
<p dir="rtl" align="right">כלולות בדיקות אוטומטיות, כולל תרחישי כשל ושירותים מדומים. חיבור מלא של Docker, Kafka ו־OpenSearch תלוי בסביבת המחשב ונבדק באמצעות תאי ההכנה והרצתכם; אין להסיק ממעבר בדיקות מדומות ששרתים חיים נבדקו.</p>
<p dir="rtl" align="right">המחברת <a href="Kafka_Python_Step_by_Step_HE.ipynb"><code dir="ltr">Kafka_Python_Step_by_Step_HE.ipynb</code></a> נשמרת <strong>לעיון בגרסה המשולבת הקודמת בלבד</strong>. מסלול הלימוד הנוכחי הוא מחברות 01 ו־02 לעיל.</p>
<p dir="rtl" align="right">המעבדה נכתבה בפייתון בהשראת <a href="https://github.com/conduktor/kafka-beginners-course">קורס Kafka למתחילים של Conduktor</a>. מקורות רשמיים והסבר מפורט נמצאים במדריך שתי המחברות.</p>
</div>
