# نشر QuotexChart على Render.com (تشغيل سحابي دائم)

## ما الذي تغيّر في الكود؟
- تم استبدال `mode='chrome'` (فتح نافذة على جهازك) بوضع **headless** (`mode=None`)
  يجعل `engine.py` يعمل كخادم ويب عادي بدون الحاجة لمتصفح مثبت على السيرفر.
- تمت إضافة قراءة البورت من متغير البيئة `PORT` (Render يحدده تلقائيًا).
- تمت إضافة **auto-login** يعمل عند إقلاع السيرفر مباشرة، بدون الحاجة لفتح
  صفحة تسجيل الدخول يدويًا في كل مرة يعيد فيها Render تشغيل الخدمة.

## خطوات النشر

### 1. ارفع المشروع على GitHub
```bash
git init
git add .
git commit -m "Cloud-ready QuotexChart"
git remote add origin https://github.com/<username>/QuotexChart.git
git push -u origin main
```
⚠️ **لا ترفع أبدًا** أي ملف يحتوي إيميلك/باسوردك (مثل `credentials.json`) —
تم إضافته لـ `.gitignore` بالفعل تحسبًا لذلك.

### 2. أنشئ خدمة جديدة على Render
1. من لوحة تحكم Render: **New → Web Service**
2. اربط مستودع GitHub بتاعك
3. **Build Command:** `pip install -r requirements.txt`
4. **Start Command:** `python engine.py` (أو اتركه فاضي، الـ `Procfile` هيتكفل بيها)
5. اختر خطة **Instance Type** — تجنب الخطة المجانية لأنها:
   - "تنام" الخدمة بعد فترة عدم استخدام (غير مناسب لبوت شغّال 24 ساعة)
   - محدودة في موارد المعالجة اللازمة لحلقة asyncio المستمرة

### 3. أضف بيانات الحساب كـ Environment Variables (وليس في الكود)
من صفحة الخدمة في Render → تبويب **Environment** → **Add Environment Variable**:

| Key | Value |
|---|---|
| `QUOTEX_EMAIL` | إيميل حسابك في Quotex |
| `QUOTEX_PASSWORD` | باسورد حسابك في Quotex |

هذه الطريقة تخزّن البيانات مشفّرة في Render نفسه، بعيدًا تمامًا عن الكود العام
على GitHub — وهذا هو السبب في عدم تضمين `credentials.json` داخل هذا المشروع.

### 4. Deploy
بعد الحفظ، Render هيعمل Deploy تلقائي. بمجرد اكتمال البناء:
- السيرفر هيسجّل الدخول تلقائيًا بالبيانات من الخطوة 3
- هتقدر تفتح الواجهة من رابط Render العام (`https://your-app.onrender.com`)
  لمتابعة الشارت حتى لو جهازك مقفول تمامًا

## ملاحظات مهمة
- **الحساب PRACTICE افتراضيًا**: الكود بينده `CLIENT.change_account("PRACTICE")`
  تلقائيًا — يعني هيشتغل على الحساب التجريبي إلا لو عدّلت هذا السطر بنفسك.
  لو قررت تحويله لحساب REAL، افهم إنك بتشغّل بوت يتفاعل مع فلوسك الحقيقية
  24 ساعة بدون إشراف مباشر منك.
- **pyquotex غير رسمية**: أي تغيير من طرف Quotex في الموقع قد يكسر الاتصال
  فجأة على السيرفر السحابي بدون تنبيه، فراقب الـ Logs من لوحة Render بشكل دوري.
- **الأمان**: لو سبق ورفعت `credentials.json` بالغلط لأي مستودع Git (حتى Private)،
  يُفضّل تغيير باسورد حساب Quotex فورًا، لأن الباسورد يبقى محفوظ في تاريخ الـ
  commits حتى لو حذفت الملف لاحقًا.
