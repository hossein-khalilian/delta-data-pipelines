from datetime import datetime, timedelta
import re


def persian_to_english_digits(s):
    """تبدیل اعداد فارسی به انگلیسی"""
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    english_digits = "0123456789"
    trans_table = str.maketrans("".join(persian_digits), "".join(english_digits))
    return s.translate(trans_table)


def text_to_date(text):
    text = text.strip()
    text = persian_to_english_digits(text)
    now = datetime.now()

    # حالت‌های خاص
    if "لحظاتی پیش" in text:
        return now
    if "دقایقی پیش" in text:
        return now - timedelta(minutes=5)

    # استخراج عدد
    match = re.search(r"(\d+)", text)
    amount = int(match.group(1)) if match else 0

    if "روز" in text:
        result = now - timedelta(days=amount)
    elif "هفته" in text:
        result = now - timedelta(weeks=amount)
    elif "ماه" in text:
        # فرض: هر ماه = 30 روز
        result = now - timedelta(days=amount * 30)
    elif "ساعت" in text:
        result = now - timedelta(hours=amount)
    else:
        result = now

    return result


# 🔹 تست چند نمونه:
samples = [
    "۲ روز پیش",
    "1 هفته پیش",
    "۲ ماه پیش",
    "لحظاتی پیش",
    "دقایقی پیش",
    "۳ ساعت پیش",
]
for s in samples:
    print(s, "=>", text_to_date(s).strftime("%Y-%m-%d %H:%M:%S"))
