FROM python:3.11-slim

WORKDIR /app

# تثبيت الاعتماديات
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ باقي الملفات
COPY . .

# إنشاء المستخدم غير الجذر أولاً
RUN useradd --create-home appuser

# إنشاء مجلد البيانات وتعيين الصلاحيات للمستخدم
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# منح المستخدم صلاحية كامل المجلد
RUN chown -R appuser:appuser /app

# التبديل إلى المستخدم غير الجذر
USER appuser

# تعريف المنفذ
EXPOSE 8501

# فحص الصحة (اختياري)
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# تشغيل التطبيق
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]