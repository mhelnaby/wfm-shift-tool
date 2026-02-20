import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import re
import json
from pathlib import Path

# استيراد مدير قاعدة البيانات
from database.db_manager import DatabaseManager

# تهيئة قاعدة البيانات
db = DatabaseManager(year=2026, db_path="/tmp/wfm_data")
db.init_database()
db.ensure_monthly_tables("2026_02")

# تهيئة session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user' not in st.session_state:
    st.session_state.user = None
if 'role' not in st.session_state:
    st.session_state.role = None

# شاشة تسجيل الدخول
if not st.session_state.authenticated:
    st.title("🔐 WFM Shift Tool - تسجيل الدخول")
    citrix_uid = st.text_input("Citrix UID")
    if st.button("دخول"):
        with db.connect() as conn:
            cur = conn.execute("SELECT * FROM user_access WHERE citrix_uid = ?", (citrix_uid,))
            user = cur.fetchone()
        if user:
            st.session_state.authenticated = True
            st.session_state.user = dict(user)
            st.session_state.role = user['role']
            st.rerun()
        else:
            st.error("المستخدم غير موجود")
    st.stop()

# بعد تسجيل الدخول
st.set_page_config(page_title="WFM Shift Tool", layout="wide")
st.title(f"📊 WFM Shift & Absenteeism Tool")
st.caption(f"مرحباً {st.session_state.user['full_name']} - ({st.session_state.role})")

# شريط جانبي للتنقل
menu_options = ["الرئيسية"]
if st.session_state.role in ['ADMIN', 'LEAVES']:
    menu_options.append("رفع الملفات")
if st.session_state.role in ['OPS', 'ADMIN']:
    menu_options.append("إدارة المناوبات")
if st.session_state.role in ['RTM', 'ADMIN']:
    menu_options.append("الموافقات")
if st.session_state.role == 'ADMIN':
    menu_options.extend(["لوحة الإدارة", "سجل التدقيق"])
menu_options.extend(["تصدير البيانات", "التقارير"])

selected = st.sidebar.radio("القوائم", menu_options)

# الصفحة الرئيسية
if selected == "الرئيسية":
    st.subheader("📈 نظرة عامة")
    st.info("هنا ستظهر إحصائيات اليوم والإشعارات لاحقاً")
    # يمكنك إضافة بعض الإحصائيات البسيطة هنا

elif selected == "رفع الملفات":
    st.subheader("📤 رفع ملفات")
    tab1, tab2, tab3 = st.tabs(["Headcount", "Roster", "CMS"])
    with tab1:
        st.write("ارفع ملف الـ Headcount")
        # أضف كود رفع الملفات لاحقاً
    with tab2:
        st.write("ارفع ملف الـ Roster")
    with tab3:
        st.write("ارفع ملف CMS")

elif selected == "إدارة المناوبات":
    st.subheader("🔄 طلبات المناوبة")
    st.info("هنا ستظهر طلبات المناوبة")

elif selected == "الموافقات":
    st.subheader("✅ الموافقات المعلقة")
    st.info("طلبات المناوبة في انتظار الموافقة")

elif selected == "لوحة الإدارة":
    st.subheader("⚙️ إعدادات النظام")
    st.info("إدارة المستخدمين، قاموس الشفتات، إلخ")

elif selected == "سجل التدقيق":
    st.subheader("📋 سجل الأحداث")
    st.info("جميع العمليات المسجلة")

elif selected == "تصدير البيانات":
    st.subheader("📥 تصدير")
    st.info("اختر نطاق التاريخ وصيغة التصدير")

elif selected == "التقارير":
    st.subheader("📋 التقارير")
    st.info("تقرير الغيابات والالتزام")