"""
تشخيص سريع — diagnose.py
"""
import json, sys, asyncio, logging

# أخمد جميع الـ loggers الخارجية
logging.disable(logging.CRITICAL)

print("\n=== QUOTEX DIAGNOSTICS ===\n")

# 1. credentials
try:
    with open('credentials.json') as f:
        creds = json.load(f)
    email = creds.get('email', '').strip()
    pwd   = creds.get('password', '').strip()
    if not email or not pwd:
        print("❌ credentials.json فارغ"); sys.exit(1)
    print(f"✓ Email: {email[:4]}***")
except FileNotFoundError:
    print("❌ credentials.json غير موجود"); sys.exit(1)

# 2. pyquotex
try:
    from pyquotex.stable_api import Quotex
    print("✓ pyquotex موجود")
except ImportError as e:
    print(f"❌ pyquotex غير موجود: {e}"); sys.exit(1)

# 3. اختبار الاتصال
print("⏳ اختبار الاتصال (30 ثانية)...")

async def test():
    import os, io, contextlib
    client = Quotex(email, pwd)
    try:
        with contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            ok, reason = await asyncio.wait_for(client.connect(), timeout=35)
        if ok:
            print("✓ الاتصال نجح!")
        else:
            print(f"❌ رُفض الاتصال: {reason}")
            print("  → Session منتهية: احذف session.json وأعد التشغيل")
    except asyncio.TimeoutError:
        print("❌ انتهت المهلة — تحقق من الإنترنت")
    except Exception as e:
        print(f"❌ خطأ: {type(e).__name__}")
    finally:
        try:
            await client.close()
        except Exception:
            pass

asyncio.run(test())
print("\n=== انتهى التشخيص ===\n")
