import streamlit as st
import pandas as pd
import numpy as np
from database.db_manager import DatabaseManager
from datetime import datetime
import io

def main(db=None):
    """
    صفحة تقارير الحضور والغياب.
    يمكن استدعاؤها من App.py مع تمرير كائن DatabaseManager أو إنشاء كائن جديد.
    """
    # إذا لم يتم تمرير db، نقوم بإنشائه (للاستخدام المستقل)
    if db is None:
        db = DatabaseManager(year=datetime.now().year)

    st.subheader("📊 تقارير الحضور والغياب")

    # اختيار الشهر والسنة
    col1, col2 = st.columns(2)
    with col1:
        year = st.number_input("السنة", min_value=2020, max_value=2030, value=datetime.now().year, key="rep_year")
    with col2:
        month = st.number_input("الشهر", min_value=1, max_value=12, value=datetime.now().month, key="rep_month")

    year_month = f"{year}_{month:02d}"

    # التأكد من وجود الجداول الشهرية
    db.ensure_monthly_tables(year_month)

    # استعلام لجلب بيانات الحضور مع معلومات الوكيل
    @st.cache_data(ttl=600)
    def load_attendance_summary(ym):
        with db.connect() as conn:
            query = f"""
                SELECT 
                    a.citrix_uid,
                    a.name AS agent_name,
                    a.team_leader,
                    a.supervisor,
                    COUNT(DISTINCT ap.shift_date) AS days_worked,
                    SUM(ap.staff_time_min) AS total_staff_hours,
                    SUM(CASE 
                        WHEN ap.attendance_status IN ('Present', 'Present - Modified') 
                        THEN 1 ELSE 0 
                    END) AS present_days,
                    SUM(CASE 
                        WHEN ap.attendance_status IN ('Absent', 'Absent - Unjustified') 
                        THEN 1 ELSE 0 
                    END) AS absent_days,
                    SUM(CASE 
                        WHEN ap.attendance_status IN ('Leave', 'Leave - Approved') 
                        THEN 1 ELSE 0 
                    END) AS leave_days
                FROM attendance_processed_{ym} ap
                JOIN agents_master a ON ap.citrix_uid = a.citrix_uid
                GROUP BY a.citrix_uid, a.name, a.team_leader, a.supervisor
            """
            df = pd.read_sql_query(query, conn)
            
            # تحويل الأعمدة الرقمية إلى أنواع رقمية
            numeric_cols = ['days_worked', 'total_staff_hours', 'present_days', 'absent_days', 'leave_days']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # حساب نسبة الحضور
            total_days = df['present_days'] + df['absent_days'] + df['leave_days']
            # تجنب القسمة على صفر
            attendance_pct = (df['present_days'] / total_days.replace(0, np.nan) * 100).round(1)
            df['attendance_percentage'] = attendance_pct.fillna(0).astype(str) + '%'
            
            return df

    try:
        df_summary = load_attendance_summary(year_month)
    except Exception as e:
        st.error(f"حدث خطأ في تحميل البيانات: {e}")
        df_summary = pd.DataFrame()

    if not df_summary.empty:
        st.subheader(f"ملخص الحضور لشهر {year_month.replace('_', '/')}")
        
        # عرض الفلاتر
        with st.expander("🔍 فلترة البيانات"):
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                team_filter = st.multiselect("فريق", options=df_summary['team_leader'].unique())
            with col_f2:
                sup_filter = st.multiselect("مشرف", options=df_summary['supervisor'].unique())
            with col_f3:
                agent_filter = st.multiselect("وكيل", options=df_summary['agent_name'].unique())
        
        # تطبيق الفلترة
        filtered_df = df_summary.copy()
        if team_filter:
            filtered_df = filtered_df[filtered_df['team_leader'].isin(team_filter)]
        if sup_filter:
            filtered_df = filtered_df[filtered_df['supervisor'].isin(sup_filter)]
        if agent_filter:
            filtered_df = filtered_df[filtered_df['agent_name'].isin(agent_filter)]
        
        # عرض الجدول
        st.dataframe(
            filtered_df[[
                'citrix_uid', 'agent_name', 'team_leader', 'supervisor',
                'days_worked', 'total_staff_hours', 'present_days', 
                'absent_days', 'leave_days', 'attendance_percentage'
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "citrix_uid": "كود الوكيل",
                "agent_name": "الاسم",
                "team_leader": "قائد الفريق",
                "supervisor": "مشرف",
                "days_worked": "أيام العمل",
                "total_staff_hours": "إجمالي الساعات",
                "present_days": "أيام حضور",
                "absent_days": "أيام غياب",
                "leave_days": "أيام إجازة",
                "attendance_percentage": "نسبة الحضور"
            }
        )
        
        # إحصائيات سريعة
        st.subheader("📈 إحصائيات سريعة")
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
        with col_s1:
            st.metric("إجمالي الوكلاء", filtered_df.shape[0])
        with col_s2:
            st.metric("إجمالي أيام الحضور", filtered_df['present_days'].sum())
        with col_s3:
            st.metric("إجمالي أيام الغياب", filtered_df['absent_days'].sum())
        with col_s4:
            st.metric("إجمالي ساعات العمل", f"{filtered_df['total_staff_hours'].sum():,.0f}")
        
        # تصدير إلى Excel
        @st.cache_data
        def convert_df_to_excel(df):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='ملخص الحضور')
            return output.getvalue()
        
        excel_data = convert_df_to_excel(filtered_df)
        st.download_button(
            label="📥 تحميل التقرير Excel",
            data=excel_data,
            file_name=f"attendance_summary_{year_month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("لا توجد بيانات لهذا الشهر. يرجى رفع بيانات الحضور أولاً.")

# إذا تم تشغيل الملف مباشرة (للتجربة المنفصلة)
if __name__ == "__main__":
    main()