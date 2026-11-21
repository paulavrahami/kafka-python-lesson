<div dir="rtl" lang="he" align="right">

<h1 dir="rtl" align="right">מחברת 03 — חיבור Wikimedia ל־Confluent Cloud ול־OpenSearch</h1>

<p dir="rtl" align="right"><strong>במחברת הזאת שרת Kafka נמצא בענן.</strong> קוד Python ממשיך לרוץ ב־Cursor במחשב שלכם, והוא קורא אירועים חיים מ־Wikimedia, שולח אותם ל־Kafka בענן, צורך אותם וכותב ל־OpenSearch. ברירת המחדל של OpenSearch נשארת השרת המקומי מהמחברת השנייה.</p>

<p dir="rtl" align="right">הקובץ להרצה הוא <a href="../03_Wikimedia_Confluent_Cloud_HE.ipynb"><code dir="ltr">03_Wikimedia_Confluent_Cloud_HE.ipynb</code></a>. המדריך מניח שכבר פתחתם את תיקיית הפרויקט ב־Cursor ושאתם משתמשים ב־<code dir="ltr">Python 3.13.12</code> וב־<code dir="ltr">uv</code>. נדרשים חשבון Confluent Cloud, אשכול ומפתח שתיצרו בחשבונכם; הקבצים במאגר אינם כוללים חשבון או מפתח פעיל.</p>

<table dir="rtl" width="100%">
<thead><tr><th dir="rtl" align="right">רכיב</th><th dir="rtl" align="right">איפה הוא פועל במסלול הזה?</th></tr></thead>
<tbody>
<tr><td dir="rtl" align="right">מקור Wikimedia</td><td dir="rtl" align="right">שירות האירועים הציבורי באינטרנט.</td></tr>
<tr><td dir="rtl" align="right">Producer ו־Consumer</td><td dir="rtl" align="right">ב־Kernel המקומי של המחברת.</td></tr>
<tr><td dir="rtl" align="right">Kafka</td><td dir="rtl" align="right">באשכול Confluent Cloud שתבחרו.</td></tr>
<tr><td dir="rtl" align="right">OpenSearch</td><td dir="rtl" align="right">מקומי, ב־<code dir="ltr">http://127.0.0.1:9200</code>, אלא אם הגדרתם יעד אחר.</td></tr>
</tbody>
</table>

<p dir="rtl" align="right">המסלול משתמש ב־JSON רגיל ובצרכן Python. אין שלב התקנה של Schema Registry, Kafka Connect או Flink. שירותים אלה אינם חלק מקוד הפרויקט.</p>

<h2 dir="rtl" align="right">1. בחירת אשכול לתרגול והבנת העלות</h2>

<p dir="rtl" align="right"><strong>ההמלצה לקורס היא אשכול Basic עם חיבור ציבורי</strong>, בסביבה שמיועדת לתרגול. זו בחירה לימודית שמצמצמת את הגדרת הרשת: מדריך ההתחלה הרשמי משתמש ב־Basic, והסוג הזה תומך בחיבור ציבורי. בחרו ספק ענן ואזור שמתאימים לכם ולמדיניות המכללה. ראו <a href="https://docs.confluent.io/cloud/current/get-started/index.html">Quick Start</a>, <a href="https://docs.confluent.io/cloud/current/clusters/cluster-types.html">סוגי אשכולות</a> ו־<a href="https://docs.confluent.io/cloud/current/networking/overview.html">אפשרויות רשת</a>.</p>

<p dir="rtl" align="right">לפני יצירת האשכול בדקו את אומדן העלות ב־Console ואת <a href="https://www.confluent.io/pricing/">התמחור העדכני</a>. חיובים עשויים לכלול קיבולת, תעבורה ואחסון; ההרכב תלוי במשאבים ובמסלול. אל תניחו שהתרגיל חינמי משום שהוא קצר או משום שמוצעת תקופת ניסיון. פירוט רשמי: <a href="https://docs.confluent.io/cloud/current/billing/dimensions/index.html">Billing Dimensions</a>.</p>

<h2 dir="rtl" align="right">2. ב־Console: יוצרים או בוחרים אשכול</h2>

<ol dir="rtl" align="right">
<li dir="rtl" align="right">היכנסו ל־<a href="https://confluent.cloud">Confluent Cloud Console</a> עם חשבון המשתמש שלכם, ובחרו Environment לתרגול.</li>
<li dir="rtl" align="right">אם אין אשכול מתאים, השתמשו ב־<code dir="ltr">Add cluster</code>. תנו שם ברור, בחרו <code dir="ltr">Basic</code>, ספק ואזור.</li>
<li dir="rtl" align="right">קראו את סיכום המשאבים והעלות לפני <code dir="ltr">Launch cluster</code>, והמתינו שהאשכול יהיה מוכן. אם נדרש אמצעי תשלום או אישור ארגוני, זהו שלב ניהול החשבון שלכם.</li>
</ol>

<p dir="rtl" align="right">הנתיב ושמות הפעולות לעיל מבוססים על <a href="https://docs.confluent.io/cloud/current/clusters/create-cluster.html">הוראות יצירת אשכול</a>. הממשק עשוי להשתנות; חפשו את פעולת יצירת האשכול בתוך הסביבה הנבחרת, ולא את יצירת מפתח API כתחליף לה.</p>

<h2 dir="rtl" align="right">3. יוצרים Topic עם שלוש מחיצות</h2>

<p dir="rtl" align="right">בתוך האשכול בחרו <code dir="ltr">Topics → Create topic</code>. שם ה־Topic עבור מחברת 03 הוא <code dir="ltr">python.wikimedia.cloud.v1</code>. הגדירו <strong>3 מחיצות</strong> לפני היצירה. אם השדה אינו מופיע במסך הראשוני, פתחו <code dir="ltr">Customize settings</code> וחפשו את הגדרת מספר המחיצות. לאחר היצירה בדקו בפרטי ה־Topic שאכן מופיעות שלוש מחיצות. אלה פעולות ה־Console המתועדות ב־<a href="https://docs.confluent.io/cloud/current/topics/overview.html">ניהול Topics</a>.</p>

<p dir="rtl" align="right">אל תסתמכו על <code dir="ltr">Create with defaults</code> בלי לבדוק את המספר: ברירת המחדל המתועדת למספר המחיצות היא 6. Confluent Cloud מנהלת Replication Factor של 3; אין להעתיק לכאן את הערך 1 מהמעבדה המקומית. מקורות: <a href="https://docs.confluent.io/cloud/current/topics/manage.html">הגדרות Topic</a> ו־<a href="https://docs.confluent.io/cloud/current/topics/topics-faq.html">שאלות נפוצות על Replication</a>.</p>

<p dir="rtl" align="right"><strong>מי יוצר את ה־Topic?</strong> משתמש Console בעל הרשאת יצירה מתאימה, בדרך כלל מנהל המעבדה. זהו שלב הקמה חד־פעמי. מחברת 03 משתמשת ב־<code dir="ltr">create_topic=False</code>: היא בודקת Topic קיים ואינה יוצרת אותו. מפתח ההרצה אינו צריך לקבל הרשאות יצירה, שינוי או מחיקה רק כדי להפעיל את המחברת. הרשאות Kafka המתאימות ליצירה ולהפעלה מפורטות ב־<a href="https://docs.confluent.io/cloud/current/security/access-control/acls/operations.html">ACL Operations</a>.</p>

<h2 dir="rtl" align="right">4. מגדירים זהות והרשאות להרצת המחברת</h2>

<p dir="rtl" align="right">צרו Service Account ייעודי, למשל <code dir="ltr">wikimedia-course</code>, דרך <code dir="ltr">Accounts &amp; access → Service accounts → Add service account</code>. הוסיפו תיאור והגדירו מי ינהל אותו. הקמת החשבון וניהולו דורשים הרשאות מנהל מתאימות; ליצירת API key עבורו נדרשים OrganizationAdmin או ResourceOwner על אותו Service Account. ההרשאות האלה שייכות לאדם שמבצע את ההקמה, ואינן הרשאות שיש לתת ליישום עצמו. ראו <a href="https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/create-service-accounts.html">יצירת Service Account</a> ו־<a href="https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html">הרשאות ליצירת מפתחות</a>.</p>

<p dir="rtl" align="right">מפתח API מאמת את הזהות; הרשאות הפעולה נקבעות לפי ה־ACL או התפקידים של בעל המפתח. עבור ההרצה הרגילה הגדירו את ההרשאות הבאות על <strong>השמות המדויקים</strong>, ללא תו כללי שמאפשר גישה לכל ה־Topics. מקור: <a href="https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html">מפתחות וזהויות</a>.</p>

<table dir="rtl" width="100%">
<thead><tr><th dir="rtl" align="right">משאב</th><th dir="rtl" align="right">שם</th><th dir="rtl" align="right">הרשאות ההרצה</th></tr></thead>
<tbody>
<tr><td dir="rtl" align="right">Topic</td><td dir="rtl" align="right"><code dir="ltr">python.wikimedia.cloud.v1</code></td><td dir="rtl" align="right"><code dir="ltr">READ, WRITE, DESCRIBE</code></td></tr>
<tr><td dir="rtl" align="right">Consumer Group</td><td dir="rtl" align="right"><code dir="ltr">python-wikimedia-cloud-opensearch-v1</code></td><td dir="rtl" align="right"><code dir="ltr">READ</code></td></tr>
</tbody>
</table>

<p dir="rtl" align="right"><code dir="ltr">DESCRIBE</code> על ה־Topic משמש גם לבדיקת ה־metadata של AdminClient. <code dir="ltr">WRITE</code> מאפשר שליחה; <code dir="ltr">READ</code> על ה־Topic ועל הקבוצה מאפשר צריכה ושמירת התקדמות. מפתח ההרצה אינו מפתח לניהול הארגון. ראו <a href="https://docs.confluent.io/cloud/current/security/access-control/acls/overview.html">עקרונות ACL</a>.</p>

<h3 dir="rtl" align="right">ב־Basic: ACL מדויק, ללא חיפוש אחר תפקיד RBAC שאינו זמין</h3>

<p dir="rtl" align="right">לפי התיעוד, RBAC ברמת משאבי Kafka פרטניים זמין ב־Standard, Enterprise, Dedicated ו־Freight, ולא ב־Basic. לכן למסלול Basic המדריך משתמש ב־ACL. מנהל מורשה יכול להגדיר אותו באמצעות Confluent CLI; זו פעולת הקמה של המנהל, ולא תלות Python נוספת למחברת. מקורות: <a href="https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html">זמינות RBAC</a>, <a href="https://docs.confluent.io/confluent-cli/current/install.html">התקנת Confluent CLI</a>.</p>

<p dir="rtl" align="right">אחרי התקנת CLI, המנהל מתחבר עם חשבון הניהול שלו. החליפו את <code dir="ltr">env-REPLACE</code>,‏ <code dir="ltr">lkc-REPLACE</code> ו־<code dir="ltr">sa-REPLACE</code> במזהי הסביבה, האשכול וחשבון השירות. הפקודות הבאות אינן מכילות API Secret:</p>

</div>

```powershell
confluent login
confluent environment use env-REPLACE
confluent kafka cluster use lkc-REPLACE
confluent kafka acl create --allow --service-account sa-REPLACE --operations read,write,describe --topic python.wikimedia.cloud.v1
confluent kafka acl create --allow --service-account sa-REPLACE --operations read --consumer-group python-wikimedia-cloud-opensearch-v1
```

<div dir="rtl" lang="he" align="right">

<p dir="rtl" align="right">תחביר הפקודות מבוסס על <a href="https://docs.confluent.io/confluent-cli/current/command-reference/kafka/acl/confluent_kafka_acl_create.html">תיעוד ACL ב־CLI</a>. לפי <a href="https://docs.confluent.io/cloud/current/security/access-control/acls/overview.html">תיעוד ניהול ACL</a>, התפקידים CloudClusterAdmin, EnvironmentAdmin ו־OrganizationAdmin יכולים לנהל ACL בהיקף המשאבים המתאים. אין צורך לתת תפקיד ניהול כזה ל־Service Account של המחברת.</p>

<h3 dir="rtl" align="right">ומה לגבי IdempotentWrite?</h3>

<p dir="rtl" align="right">ה־Producer בפרויקט משתמש ב־<code dir="ltr">enable.idempotence=True</code>. ב־Kafka עם מנגנון ההרשאות המעודכן מגרסה 2.8, הרשאת Write על Topic יכולה לאפשר את אתחול המפיק האידמפוטנטי; אין להסיק שכל מפיק כזה דורש תמיד הרשאת אשכול נוספת. זה מתועד ב־<a href="https://cwiki.apache.org/confluence/spaces/KAFKA/pages/165221843/KIP-679%2BProducer%2Bwill%2Benable%2Bthe%2Bstrongest%2Bdelivery%2Bguarantee%2Bby%2Bdefault">Apache KIP-679</a>, וב־<a href="https://docs.confluent.io/platform/8.4/security/authorization/rbac/rbac-cli-quickstart.html">תיעוד Confluent Platform</a> שמתאר Write על Topic או IdempotentWrite על האשכול.</p>

<p dir="rtl" align="right">תיעוד Confluent Cloud עדיין מונה <code dir="ltr">IDEMPOTENT_WRITE</code> כפעולה נתמכת על Cluster. לכן מתחילים בהרשאות המדויקות שבטבלה; אם אתחול <code dir="ltr">InitProducerId</code> נכשל ב־<code dir="ltr">CLUSTER_AUTHORIZATION_FAILED</code>, המנהל בודק את ההרשאות ואת מדיניות ה־authorizer. כאשר נדרשת הרשאה נוספת, נותנים רק <code dir="ltr">IDEMPOTENT_WRITE</code> על האשכול המתאים. אין להחליף זאת בהרשאת מנהל גורפת או בכיבוי האידמפוטנטיות. מקור: <a href="https://docs.confluent.io/cloud/current/security/access-control/acls/operations.html">פעולות Cluster ב־Cloud</a>.</p>

<h2 dir="rtl" align="right">5. יוצרים מפתח API מסוג Kafka לאשכול הנכון</h2>

<ol dir="rtl" align="right">
<li dir="rtl" align="right">פתחו את <a href="https://confluent.cloud/settings/api-keys">API keys</a> ובחרו <code dir="ltr">Add API key</code>.</li>
<li dir="rtl" align="right">בחרו את ה־Service Account שהכנתם.</li>
<li dir="rtl" align="right">ב־<code dir="ltr">Resource scope</code> בחרו <strong>Kafka cluster</strong>, ואז את הסביבה והאשכול של התרגיל.</li>
<li dir="rtl" align="right">צרו את המפתח ושמרו את ה־API Key וה־API Secret במקום פרטי. ה־Secret מוצג בעת היצירה ואינו ניתן לשליפה שוב אחרי סיום המסך.</li>
</ol>

<p dir="rtl" align="right">אלה השלבים ב־<a href="https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html">Manage API Keys</a>. המפתח למסלול הזה משויך ל־Kafka cluster. מפתח ל־Cloud resource management, ל־Schema Registry או למשאב אחר אינו התצורה של המחברת.</p>

<h2 dir="rtl" align="right">6. מעתיקים את כתובת החיבור</h2>

<p dir="rtl" align="right">בתוך האשכול פתחו <code dir="ltr">Clients</code>, בחרו לקוח Python והעתיקו את הערך של <code dir="ltr">bootstrap.servers</code>. אפשר להשתמש במפתח הקיים שיצרתם. הנתיב הזה מתועד ב־<a href="https://docs.confluent.io/cloud/current/client-apps/config-client.html">Kafka Client Quick Start</a>.</p>

<p dir="rtl" align="right">הערך הוא שם שרת ופורט, לדוגמה <code dir="ltr">pkc-XXXXX.REGION.PROVIDER.confluent.cloud:9092</code>. מעתיקים את הכתובת האמיתית מהחשבון, ללא <code dir="ltr">https://</code>, ללא נתיב וללא החלפה בכתובת IP. פרוטוקול Kafka משתמש ב־9092; כתובת REST על 443 אינה כתובת ה־bootstrap של לקוח Kafka. שמות השרת משמשים גם ל־TLS ול־SNI. מקור: <a href="https://docs.confluent.io/cloud/current/client-apps/client-faq.html">Client FAQ: פורטים ושמות שרת</a>.</p>

<p dir="rtl" align="right">המדריך מניח Endpoint ציבורי. אשכול עם Private Networking דורש נתיב רשת ו־DNS מתאימים מהמחשב שמריץ את ה־Kernel; עצם ההתחברות ל־Console או ל־VPN כלשהו אינה מוכיחה שקיים נתיב כזה. ברשת מכללה או חברה יש לוודא גישה ל־bootstrap ולכתובות ה־Brokers שמתגלות בהמשך. ראו <a href="https://docs.confluent.io/cloud/current/networking/overview.html">רשת ציבורית ופרטית</a> ו־<a href="https://docs.confluent.io/cloud/current/networking/testing.html">בדיקת קישוריות</a>.</p>

<h2 dir="rtl" align="right">7. ב־Cursor: מכינים קובץ חיבור מקומי</h2>

<p dir="rtl" align="right">מתוך תיקיית הפרויקט הריצו במסוף:</p>

</div>

```powershell
uv python install 3.13.12
uv sync --locked
uv run --locked python scripts/opensearch_docker.py start
```

<div dir="rtl" lang="he" align="right">

<p dir="rtl" align="right">Docker Desktop נדרש כאן עבור OpenSearch המקומי. אין צורך להפעיל גם את מכולת Kafka המקומית כדי להשתמש במחברת הענן.</p>

<p dir="rtl" align="right">העתיקו את <a href="../confluent-cloud.properties.example"><code dir="ltr">confluent-cloud.properties.example</code></a> לקובץ פרטי בשם <code dir="ltr">confluent-cloud.properties</code> בשורש הפרויקט. אם הקובץ הפרטי כבר קיים, שמרו אותו ואל תעתיקו עליו את הדוגמה:</p>

</div>

```powershell
if (-not (Test-Path .\confluent-cloud.properties)) {
    Copy-Item .\confluent-cloud.properties.example .\confluent-cloud.properties
}
git check-ignore -v confluent-cloud.properties
```

<div dir="rtl" lang="he" align="right">

<p dir="rtl" align="right">הפקודה האחרונה אמורה להציג כלל שמתעלם מהקובץ. היא אינה מדפיסה את תוכנו. את ערכי החיבור עורכים <strong>רק בקובץ הפרטי</strong>, ולא בקובץ <code dir="ltr">.example</code> שנשמר במאגר. אל תשלחו את הקובץ לצ׳אט, אל תדפיסו את מילון ההגדרות במחברת ואל תצרפו צילום שבו מופיע ה־Secret.</p>

<p dir="rtl" align="right">התוכן הבסיסי נראה כך; אלה מצייני מקום בלבד. החליפו אותם בערכים שקיבלתם מה־Console, ללא מרכאות מסביב לערכים:</p>

</div>

```properties
bootstrap.servers=<BOOTSTRAP_DNS_NAME>:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=REPLACE_WITH_KAFKA_API_KEY
sasl.password=REPLACE_WITH_KAFKA_API_SECRET
```

<div dir="rtl" lang="he" align="right">

<p dir="rtl" align="right"><code dir="ltr">sasl.username</code> הוא ה־API Key, ו־<code dir="ltr">sasl.password</code> הוא ה־API Secret — אלה אינם כתובת המייל וסיסמת ההתחברות ל־Console. <code dir="ltr">SASL_SSL</code> מציין אימות SASL מעל חיבור TLS, ו־<code dir="ltr">PLAIN</code> הוא מנגנון האימות בתוך החיבור המוצפן. מקור: <a href="https://docs.confluent.io/kafka-clients/python/current/index.html">תצורת לקוח Python עבור Confluent Cloud</a>.</p>

<p dir="rtl" align="right">הטוען של הפרויקט מחייב את תצורת האימות הזאת ומשאיר בדיקת תעודה ושם שרת פעילה. אם מתקבלת שגיאת תעודה, בדקו את מאגר ה־CA המקומי ואת מדיניות הרשת; אל תבטלו את אימות TLS. בגרסאות librdkafka חדשות גילוי ה־CA בדרך כלל אוטומטי, אך עדיין נדרש מאגר אמון תקין. פירוט: <a href="https://docs.confluent.io/cloud/current/client-apps/client-configs.html">TLS, SNI ותעודות</a>.</p>

<h2 dir="rtl" align="right">8. מפעילים את מחברת 03 בהדרגה</h2>

<ol dir="rtl" align="right">
<li dir="rtl" align="right">פתחו <code dir="ltr">03_Wikimedia_Confluent_Cloud_HE.ipynb</code> ובחרו Kernel מתוך <code dir="ltr">.venv</code> של הפרויקט.</li>
<li dir="rtl" align="right">הריצו את תא ההגדרות. הוא טוען את הקובץ הפרטי; אין להוסיף <code dir="ltr">print</code> של תוכנו לצורך בדיקה.</li>
<li dir="rtl" align="right">הריצו את בדיקת החיבורים. היא צריכה למצוא את ה־Topic הקיים עם שלוש מחיצות ואת יעד OpenSearch. בדיקת metadata אינה מוכיחה עדיין שהשליחה והצריכה מורשות.</li>
<li dir="rtl" align="right">רק לאחר שהבדיקה עברה, הפעילו את תא ההזרמה. הוא פועל עד לעצירה או לשגיאה מפורשת, ללא תקרה כוללת של אירועים או זמן.</li>
<li dir="rtl" align="right">עקבו אחרי <code dir="ltr">accepted</code>,‏ <code dir="ltr">confirmed</code>,‏ <code dir="ltr">indexed</code> ו־<code dir="ltr">committed</code>. עצרו באמצעות כפתור העצירה והמתינו לסגירת הלקוחות.</li>
<li dir="rtl" align="right">אחרי העצירה הריצו את תאי OpenSearch: ספירת מסמכים, חיפוש וסיכומים. אם Cursor ביטל את המשך <code dir="ltr">Run All</code>, הריצו תאים אלה ידנית.</li>
</ol>

<table dir="rtl" width="100%">
<thead><tr><th dir="rtl" align="right">הגדרה במחברת 03</th><th dir="rtl" align="right">ערך ברירת המחדל</th></tr></thead>
<tbody>
<tr><td dir="rtl" align="right">Topic בענן</td><td dir="rtl" align="right"><code dir="ltr">python.wikimedia.cloud.v1</code></td></tr>
<tr><td dir="rtl" align="right">Consumer Group</td><td dir="rtl" align="right"><code dir="ltr">python-wikimedia-cloud-opensearch-v1</code></td></tr>
<tr><td dir="rtl" align="right">Index ב־OpenSearch</td><td dir="rtl" align="right"><code dir="ltr">wikimedia-cloud-recentchanges-v1</code></td></tr>
</tbody>
</table>

<p dir="rtl" align="right">הצרכן כותב את המסמך ורק לאחר אישור הכתיבה שומר Commit ב־Kafka. מזהה המסמך הקבוע מאפשר לכתיבה חוזרת לעדכן אותו מסמך. מספר המסמכים אינו חייב להיות שווה למספר פעולות הכתיבה: ייתכנו כתיבות חוזרות ומסמכים מהרצות קודמות. גם לאחר ניקוז מוגבל בזמן בסיום ייתכן שנשארו רשומות ב־Kafka להמשך. ההסבר המלא נמצא ב־<a href="NOTEBOOK_WALKTHROUGH_HE.md">מדריך המחברות</a>.</p>

<h2 dir="rtl" align="right">9. אם עובדים כמה סטודנטים באותו אשכול</h2>

<p dir="rtl" align="right">ברירות המחדל מתאימות לתרגול נפרד. באשכול כיתתי משותף הקצו לכל סטודנט Topic ו־Consumer Group נפרדים, ועדכנו יחד את שמות המשאבים במחברת ואת ה־ACL שלהם. צרכנים שמשתמשים באותו שם קבוצה חולקים את העבודה; הם אינם מבטיחים שכל מחשב יקבל עותק מלא ל־OpenSearch המקומי שלו. מומלץ גם שם Index נפרד לכל תרגיל. אין לשתף API Secret משותף בצ׳אט הכיתה.</p>

<h2 dir="rtl" align="right">10. אבחון לפי סוג השגיאה</h2>

<table dir="rtl" width="100%">
<thead><tr><th dir="rtl" align="right">תסמין</th><th dir="rtl" align="right">מה בודקים?</th></tr></thead>
<tbody>
<tr><td dir="rtl" align="right">קובץ חסר או מצייני מקום</td><td dir="rtl" align="right">שהקובץ הפרטי נקרא <code dir="ltr">confluent-cloud.properties</code>, נמצא בתיקיית הפרויקט, וכולל את ערכי החשבון האמיתיים.</td></tr>
<tr><td dir="rtl" align="right"><code dir="ltr">SASL_AUTHENTICATION_FAILED</code></td><td dir="rtl" align="right">זוג Key ו־Secret תואם, Scope מסוג Kafka cluster ואשכול תואם ל־bootstrap. מפתח חדש עשוי להצריך זמן קצר להפצת ההגדרה; אין להדפיס את הסוד כדי לבדוק זאת.</td></tr>
<tr><td dir="rtl" align="right"><code dir="ltr">TOPIC_AUTHORIZATION_FAILED</code></td><td dir="rtl" align="right">שם Topic מדויק והרשאות Read, Write ו־Describe לזהות שבבעלותה המפתח.</td></tr>
<tr><td dir="rtl" align="right"><code dir="ltr">GROUP_AUTHORIZATION_FAILED</code></td><td dir="rtl" align="right">הרשאת Read על שם הקבוצה המדויק שמוגדר במחברת.</td></tr>
<tr><td dir="rtl" align="right">כשל אתחול Producer עם הרשאת אשכול</td><td dir="rtl" align="right">ראו את הסעיף על IdempotentWrite. בדקו את מדיניות ההרשאות במקום לתת הרשאת מנהל גורפת.</td></tr>
<tr><td dir="rtl" align="right">Timeout או כשל DNS</td><td dir="rtl" align="right">שם ה־bootstrap, יציאה לפורט 9092, גישה ל־Brokers ומדיניות Firewall/VPN. ב־Private Networking יש לבדוק נתיב רשת ו־DNS פרטיים.</td></tr>
<tr><td dir="rtl" align="right">שגיאת TLS או תעודה</td><td dir="rtl" align="right">שם השרת המקורי, מאגר תעודות אמין ותמיכת הרשת ב־SNI. אין לכבות אימות תעודה.</td></tr>
<tr><td dir="rtl" align="right">OpenSearch אינו זמין</td><td dir="rtl" align="right">Docker Desktop והפקודות <code dir="ltr">scripts/opensearch_docker.py status</code> ו־<code dir="ltr">logs</code>, כפי שמוסבר במדריך ההתקנה המקומי.</td></tr>
</tbody>
</table>

<p dir="rtl" align="right">מקורות לאבחון שירות הענן: <a href="https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html">הפצת מפתחות</a>, <a href="https://docs.confluent.io/cloud/current/security/access-control/acls/operations.html">הרשאות Kafka</a>, <a href="https://docs.confluent.io/cloud/current/networking/testing.html">בדיקת רשת</a>, <a href="https://docs.confluent.io/cloud/current/client-apps/client-configs.html">TLS ו־SNI</a>. הצלחת בדיקת פורט בלבד אינה מוכיחה שהמפתח מורשה לשלוח ולצרוך.</p>

<h2 dir="rtl" align="right">11. סיום התרגול, נתונים וחיוב</h2>

<p dir="rtl" align="right"><strong>עצירת המחברת אינה מוחקת את אשכול הענן ואינה מבטיחה שהחיוב נעצר.</strong> גם עצירת Docker המקומי אינה משנה את משאבי Confluent Cloud. בדקו ב־Console את המשאבים ואת מצב החיוב. כדי לסיים שימוש בתרגיל חד־פעמי, מנהל החשבון יכול להסיר את משאבי המעבדה שכבר אינם נחוצים, לאחר החלטה לשמור או לוותר על הנתונים; מחיקת אשכול מסירה את הנתונים שבו. אין לבצע מחיקה כזאת באשכול שמשותף לאחרים. מקורות: <a href="https://docs.confluent.io/cloud/current/billing/billing-faq.html">Billing FAQ</a> ו־<a href="https://docs.confluent.io/cloud/current/billing/dimensions/index.html">חיובים לפי משאב ושימוש</a>.</p>

<p dir="rtl" align="right">המשך עם אותה קבוצת צרכנים יכול לקרוא מהמיקום השמור כל עוד הרשומות וה־offset עדיין זמינים. סמן Wikimedia נשמר בזיכרון של אותה הרצה בלבד; אין כאן הבטחה לשחזור אירועי המקור שהתרחשו בזמן שהיישום היה כבוי. הכנסת Kafka לענן אינה משנה את המגבלה הזאת בקוד המקור.</p>

<p dir="rtl" align="right"><strong>מצב האימות של החומר:</strong> הוראות שירות הענן נבדקו מול תיעוד רשמי בתאריך 21 בנובמבר 2026. ללא חשבון ומפתחות אמיתיים אין בחומר זה הוכחה לחיבור חי אל אשכול Confluent Cloud שלכם. בדיקת החיבור, השליחה והצריכה מתבצעת אצלכם לאחר ההגדרה.</p>

</div>
