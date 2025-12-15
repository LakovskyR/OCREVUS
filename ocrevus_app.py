# -*- coding: utf-8 -*-
"""
Ocrevus Automation Script
Fetches data from Tableau, generates charts, sends sectorized emails
"""

import os
import sys
import io
import time
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

try:
    import tableauserverclient as TSC
    import pandas as pd
    import plotly.graph_objects as go
    import plotly.express as px
    from perplexity import Perplexity
except ImportError as e:
    print(f"❌ Missing libraries: {e}")
    sys.exit(1)

# ============================================================================
# CONFIG
# ============================================================================

# Tableau
TOKEN_NAME = os.getenv('TABLEAU_TOKEN_NAME')
TOKEN_SECRET = os.getenv('TABLEAU_TOKEN_SECRET')
SITE_ID = os.getenv('TABLEAU_SITE_ID')
SERVER_URL = os.getenv('TABLEAU_SERVER_URL')
WORKBOOK_NAME = 'ocrevusMailData'
VIEW_NAME = 'whole'

# Email
SENDER_EMAIL = os.getenv('GMAIL_USER')
APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')

# Perplexity
PPLX_API_KEY = os.getenv('PERPLEXITY_API_KEY')

# Active mode
ACTIVE_GROUP = os.getenv('ACTIVE_RECIPIENT_GROUP', 'test_1')

# Colors
COLORS = {
    'ocrevus_sc': '#ffc72a',
    'ocrevus_iv': '#646db1'
}

# Chart config
FONT_FAMILY = 'Arial'  # GitHub Actions doesn't have Roche Sans
CHART_TITLE_SIZE = 18
CHART_TEXT_MAIN = 14
CHART_ANNOTATION = 15
CHART_TEXT_STANDARD = 13

print(f"--- Starting Ocrevus Report ({ACTIVE_GROUP}) ---")

# ============================================================================
# FUNCTIONS
# ============================================================================

def connect_tableau():
    """Connect to Tableau and download data"""
    print(f"--- Connecting to Tableau: {SERVER_URL} ---")
    
    auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_ID)
    server = TSC.Server(SERVER_URL, use_server_version=True)
    
    with server.auth.sign_in(auth):
        req = TSC.RequestOptions()
        req.filter.add(TSC.Filter(
            TSC.RequestOptions.Field.Name,
            TSC.RequestOptions.Operator.Equals,
            WORKBOOK_NAME
        ))
        
        workbooks, _ = server.workbooks.get(req)
        if not workbooks:
            raise Exception(f"Workbook '{WORKBOOK_NAME}' not found")
        
        target_view = None
        for wb in workbooks:
            server.workbooks.populate_views(wb)
            for view in wb.views:
                if view.name == VIEW_NAME:
                    target_view = view
                    break
            if target_view:
                break
        
        if not target_view:
            raise Exception(f"View '{VIEW_NAME}' not found")
        
        print(f"✓ Downloading data from view: {VIEW_NAME}")
        server.views.populate_csv(target_view)
        csv_data = io.StringIO(b"".join(target_view.csv).decode("utf-8"))
        return pd.read_csv(csv_data)

def process_data(df_raw):
    """Unpivot and clean data"""
    print("--- Processing Data ---")
    
    dimension_cols = [col for col in df_raw.columns 
                     if col not in ['Measure Names', 'Measure Values']]
    
    df = df_raw.pivot(
        index=dimension_cols,
        columns='Measure Names',
        values='Measure Values'
    ).reset_index()
    
    df.columns.name = None
    
    # Rename
    df = df.rename(columns={
        'Day of Date Day': 'date_day',
        'Center Cip': 'center_cip',
        'Center Name': 'center_name',
        'Chainage Cip': 'chainage_cip',
        'Chainage Name': 'chainage_name',
        'Rating': 'category',
        'Secteur Promo': 'secteur_promo',
        'Secteur Medical': 'secteur_medical',
        'Secteur Ma': 'secteur_ma',
        'Email Promo': 'email_promo',
        'Email Medical': 'email_medical',
        'Email Ma': 'email_ma',
        'Volume Ocrevus Iv': 'volume_iv',
        'Volume Ocrevus Sc': 'volume_sc'
    })
    
    # Parse French dates
    month_map = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }
    
    def parse_date(date_str):
        try:
            parts = date_str.strip().split()
            day, month_fr, year = parts[0], parts[1].rstrip(',').lower(), parts[2]
            month_num = month_map.get(month_fr, month_fr)
            return pd.to_datetime(f"{year}-{month_num}-{day.zfill(2)}")
        except:
            return pd.NaT
    
    df['date_day'] = df['date_day'].apply(parse_date)
    df['volume_iv'] = df['volume_iv'].fillna(0)
    df['volume_sc'] = df['volume_sc'].fillna(0)
    
    return df

def calculate_metrics(df):
    """Calculate all metrics and build final table"""
    print("--- Calculating Metrics ---")
    
    today = datetime.now().date()
    yesterday_date = today - timedelta(days=3 if today.weekday() == 0 else 1)
    
    # Yesterday data
    if today.weekday() == 0:  # Monday
        df_yesterday = df[(df['date_day'].dt.date >= yesterday_date) & 
                         (df['date_day'].dt.date < today)].copy()
    else:
        df_yesterday = df[df['date_day'].dt.date == yesterday_date].copy()
    
    df_table = df_yesterday.groupby(['chainage_cip', 'chainage_name']).agg({
        'volume_iv': 'sum',
        'volume_sc': 'sum'
    }).reset_index()
    
    df_table.columns = [
        'chainage_cip', 'chainage_name',
        'Volume MTT Ocrevus IV de la veille',
        'Volume MTT Ocrevus SC de la veille'
    ]
    
    # MTD
    current_month = today.replace(day=1)
    df_mtd = df[df['date_day'].dt.date >= current_month].copy()
    
    df_mtd_agg = df_mtd.groupby('chainage_cip').agg({
        'volume_iv': 'sum',
        'volume_sc': 'sum',
        'center_cip': 'count'
    }).reset_index()
    
    df_mtd_agg.columns = ['chainage_cip', 'volume_iv_mtd', 'volume_sc_mtd', 'nb_orders_mtd']
    
    # 4-month avg
    start_4m = current_month - timedelta(days=120)
    df_4m = df[(df['date_day'].dt.date >= start_4m) & 
               (df['date_day'].dt.date < current_month)].copy()
    
    df_4m_agg = df_4m.groupby('chainage_cip').agg({
        'volume_iv': 'sum',
        'volume_sc': 'sum'
    }).reset_index()
    
    df_4m_agg['avg_4m'] = (df_4m_agg['volume_iv'] + df_4m_agg['volume_sc']) / 4.0
    
    # First SC order
    df_first_sc = df[df['volume_sc'] > 0].groupby('chainage_cip')['date_day'].min().reset_index()
    df_first_sc.columns = ['chainage_cip', 'date_first_sc']
    
    # Category
    cats = df.groupby('chainage_cip')['category'].first().reset_index()
    
    # Sector info
    sector_info = df[['chainage_cip', 'secteur_promo', 'email_promo', 
                      'email_medical', 'email_ma']].drop_duplicates('chainage_cip')
    
    # Merge all - USE CONSISTENT chainage_cip (lowercase)
    final = df_table.merge(df_mtd_agg, on='chainage_cip', how='left') \
                    .merge(df_4m_agg[['chainage_cip', 'avg_4m']], on='chainage_cip', how='left') \
                    .merge(df_first_sc, on='chainage_cip', how='left') \
                    .merge(cats, on='chainage_cip', how='left') \
                    .merge(sector_info, on='chainage_cip', how='left')
    
    # Format columns
    final['Volume MTT Ocrevus IV+SC dans le mois'] = \
        final['volume_iv_mtd'].fillna(0) + final['volume_sc_mtd'].fillna(0)
    
    final['Nombre de commandes dans le mois d\'Ocrevus IV+SC'] = \
        final['nb_orders_mtd'].fillna(0).astype(int)
    
    final['Date 1ère commande Ocrevus SC'] = \
        final['date_first_sc'].dt.strftime('%d/%m/%Y').fillna('')
    
    final['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'] = \
        final['avg_4m'].fillna(0).round(2)
    
    final['Catégorie de centres'] = final['category'].fillna('N/A')
    
    # Rename for display
    final = final.rename(columns={'chainage_name': 'Chainage'})
    
    # Select final columns
    final = final[[
        'Chainage', 'chainage_cip', 'Catégorie de centres',
        'secteur_promo', 'email_promo', 'email_medical', 'email_ma',
        'Volume MTT Ocrevus SC de la veille',
        'Volume MTT Ocrevus IV de la veille',
        'Volume MTT Ocrevus IV+SC dans le mois',
        'Nombre de commandes dans le mois d\'Ocrevus IV+SC',
        'Date 1ère commande Ocrevus SC',
        'Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'
    ]]
    
    return final.fillna('')

def generate_charts(df_table, df_full):
    """Generate all 4 charts"""
    print("--- Generating Charts ---")
    
    # Chart 1: KPI
    df_sc_centers = df_full[df_full['volume_sc'] > 0][['chainage_cip', 'category']].drop_duplicates()
    df_kpi = df_sc_centers.groupby('category').size().reset_index(name='Nombre de centres')
    df_kpi = df_kpi.rename(columns={'category': 'Catégorie'})
    
    all_cats = pd.DataFrame({'Catégorie': ['C1', 'C2', 'C3', 'C4']})
    df_kpi = all_cats.merge(df_kpi, on='Catégorie', how='left')
    df_kpi['Nombre de centres'] = df_kpi['Nombre de centres'].fillna(0).astype(int)
    total_hco = df_kpi['Nombre de centres'].sum()
    
    fig_kpi = px.bar(df_kpi, x='Catégorie', y='Nombre de centres',
                     color_discrete_sequence=[COLORS['ocrevus_sc']], 
                     text='Nombre de centres')
    
    fig_kpi.update_layout(
        template='plotly_white', height=450, width=600,
        font=dict(family=FONT_FAMILY, size=CHART_TEXT_MAIN),
        title=dict(text='Centres initiateurs SC', 
                  font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY)),
        yaxis=dict(rangemode='tozero', tick0=0, dtick=1, title=None),
        xaxis=dict(title=None),
        margin=dict(b=140, t=80)
    )
    
    fig_kpi.add_annotation(
        text=f'<b>{total_hco}</b>', xref="paper", yref="paper",
        x=0.95, y=1.0, showarrow=False, 
        font=dict(size=36, family=FONT_FAMILY), xanchor='right'
    )
    
    fig_kpi.add_annotation(
        text="Ambition : 70% des C1/C2 et 50% des C3 ont commandé Ocrevus SC<br>dans les 4 mois suivants le lancement soit 119 centres",
        xref="paper", yref="paper", x=0.5, y=-0.28,
        font=dict(size=CHART_ANNOTATION, family=FONT_FAMILY)
    )
    
    fig_kpi.write_image('/tmp/kpi_chart.png', scale=2)
    
    # Chart 2: Pie
    current_month = datetime.now().replace(day=1).date()
    df_mtd = df_full[df_full['date_day'].dt.date >= current_month]
    vol_iv = df_mtd['volume_iv'].sum()
    vol_sc = df_mtd['volume_sc'].sum()
    
    labels = ['IV', 'SC'] if vol_sc > 0 else ['IV']
    values = [vol_iv, vol_sc] if vol_sc > 0 else [vol_iv]
    colors = [COLORS['ocrevus_iv'], COLORS['ocrevus_sc']] if vol_sc > 0 else [COLORS['ocrevus_iv']]
    
    fig_vol = go.Figure(data=[go.Pie(
        labels=labels, values=values, marker=dict(colors=colors),
        textinfo='label+value+percent',
        texttemplate='%{label}<br>%{value:,.0f}<br>(%{percent})',
        textfont=dict(size=CHART_TEXT_MAIN, family=FONT_FAMILY)
    )])
    
    fig_vol.update_layout(
        title=dict(text='Volumes Ocrevus SC/IV - Mois en cours',
                  x=0.5, font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY)),
        template='plotly_white', height=450, width=600,
        margin=dict(l=20, r=20, t=80, b=170), showlegend=False
    )
    
    fig_vol.write_image('/tmp/vol_chart.png', scale=2)
    
    # Chart 3: Daily
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_weekday = yesterday.weekday()
    
    if yesterday_weekday >= 5:
        last_friday = yesterday - timedelta(days=yesterday_weekday - 4)
    else:
        last_friday = yesterday
    
    business_days = []
    current_day = last_friday
    while len(business_days) < 5:
        if current_day.weekday() < 5:
            business_days.insert(0, current_day)
        current_day -= timedelta(days=1)
    
    df_daily = df_full[df_full['date_day'].dt.date.isin([d.date() for d in business_days])].copy()
    df_daily = df_daily.groupby('date_day').agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_daily = df_daily.sort_values('date_day')
    df_daily['Day_Name'] = df_daily['date_day'].dt.strftime('%a')
    
    fig_daily = go.Figure()
    
    fig_daily.add_trace(go.Bar(
        x=df_daily['Day_Name'], y=df_daily['volume_iv'],
        marker=dict(color=COLORS['ocrevus_iv']),
        text=df_daily['volume_iv'].astype(int),
        textposition='inside', textfont=dict(size=CHART_TEXT_STANDARD, color='white'),
        showlegend=False
    ))
    
    if df_daily['volume_sc'].sum() > 0:
        fig_daily.add_trace(go.Bar(
            x=df_daily['Day_Name'], y=df_daily['volume_sc'],
            marker=dict(color=COLORS['ocrevus_sc']),
            text=df_daily['volume_sc'].astype(int),
            textposition='inside', textfont=dict(size=CHART_TEXT_STANDARD),
            showlegend=False
        ))
    
    fig_daily.update_layout(
        title=dict(text='Daily Ocre SC IV', 
                  font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY)),
        barmode='stack', template='plotly_white', height=400, width=900,
        xaxis=dict(title='', tickfont=dict(size=CHART_TEXT_STANDARD)),
        yaxis=dict(visible=False),
        font=dict(family=FONT_FAMILY, size=CHART_TEXT_STANDARD)
    )
    
    fig_daily.write_image('/tmp/daily_chart.png', scale=2)
    
    # Chart 4: Monthly
    df_full['month'] = df_full['date_day'].dt.to_period('M')
    df_monthly = df_full.groupby('month').agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index().tail(12)
    df_monthly['Month_Label'] = df_monthly['month'].dt.strftime('%m/%y')
    
    fig_monthly = go.Figure()
    
    fig_monthly.add_trace(go.Bar(
        x=df_monthly['Month_Label'], y=df_monthly['volume_iv'],
        marker=dict(color=COLORS['ocrevus_iv']),
        text=[f'{v/1000:.2f}K' for v in df_monthly['volume_iv']],
        textposition='outside', showlegend=False
    ))
    
    fig_monthly.add_trace(go.Bar(
        x=df_monthly['Month_Label'], y=df_monthly['volume_sc'],
        marker=dict(color=COLORS['ocrevus_sc']),
        text=[f'{v/1000:.2f}K' if v > 0 else '' for v in df_monthly['volume_sc']],
        textposition='outside', showlegend=False
    ))
    
    fig_monthly.update_layout(
        title=dict(text='Monthly Ocre SC IV',
                  font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY)),
        barmode='stack', template='plotly_white', height=400, width=900,
        xaxis=dict(title=''),
        yaxis=dict(title='Total Qté UE Mois'),
        font=dict(family=FONT_FAMILY, size=CHART_TEXT_STANDARD)
    )
    
    fig_monthly.write_image('/tmp/monthly_chart.png', scale=2)
    
    return {
        'total_hco': total_hco,
        'vol_iv': int(vol_iv),
        'vol_sc': int(vol_sc),
        'vol_total': int(vol_iv + vol_sc)
    }

def generate_ai_content(metrics):
    """Generate P.S. content with Perplexity"""
    print("--- Generating AI Commentary ---")
    
    try:
        client = Perplexity(api_key=PPLX_API_KEY)
        
        today = datetime.now()
        ps_prompt = f"""Ocrevus (SEP):
- Ocrevus IV: existant
- Ocrevus SC: nouvelle forme lancée 12/12/2024

Situation {today.strftime('%d/%m/%Y')}:
- IV: {metrics['vol_iv']} unités
- SC: {metrics['vol_sc']} unités
- Centres SC: {metrics['total_hco']}

En 2-3 phrases FR (sans markdown): dynamique SC vs IV, trajectoire OK?"""
        
        completion = client.chat.completions.create(
            messages=[{"role": "user", "content": ps_prompt}],
            model="sonar"
        )
        
        ps_content = completion.choices[0].message.content
        ps_content = ps_content.replace('**', '').replace('*', '').replace('#', '')
        return ps_content
    except Exception as e:
        print(f"⚠ AI error: {e}")
        return f"Performance actuelle: {metrics['vol_total']} unités ce mois. Lancement SC 12/12/2024, montée en charge en cours."

def build_html(table):
    """Build HTML email"""
    table_sorted = table.sort_values('Volume MTT Ocrevus IV+SC dans le mois', ascending=False)
    
    rows = ""
    for _, r in table_sorted.iterrows():
        rows += f"""<tr>
<td style="font-size:11px">{r['Chainage']}</td>
<td style="text-align:center;font-size:11px">{r['Catégorie de centres']}</td>
<td style="text-align:center;font-size:11px">{r['Volume MTT Ocrevus SC de la veille']}</td>
<td style="text-align:center;font-size:11px">{r['Volume MTT Ocrevus IV de la veille']}</td>
<td style="text-align:center;font-weight:bold;font-size:11px">{r['Volume MTT Ocrevus IV+SC dans le mois']}</td>
<td style="text-align:center;font-size:11px">{r["Nombre de commandes dans le mois d'Ocrevus IV+SC"]}</td>
<td style="text-align:center;font-size:11px">{r['Date 1ère commande Ocrevus SC']}</td>
<td style="text-align:center;font-size:11px">{r['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois']}</td>
</tr>"""
    
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
body{{font-family:Arial;margin:0;padding:0;background:#f5f5f3}}
.container{{max-width:900px;margin:0 auto;background:white}}
.content{{padding:20px 40px}}
.intro-text{{font-size:14px;line-height:1.8;margin-bottom:20px}}
.section-title{{font-size:18px;font-weight:bold;margin:30px 0 15px 0}}
table{{width:100%;border-collapse:collapse;margin:20px 0;font-size:11px}}
th{{background:#646db1;color:white;padding:12px 8px;text-align:center;font-weight:bold;border:1px solid #5560a0;font-size:11px}}
td{{padding:10px 8px;border:1px solid #e0e0e0}}
tr:nth-child(even){{background:#f9f9f9}}
.kpi-container{{display:flex;justify-content:space-between;margin:20px 0;gap:20px}}
.kpi-card{{flex:1}}
.chart{{text-align:center;margin:20px 0}}
.signature{{margin-top:30px;font-size:14px;line-height:1.8}}
.ps{{margin-top:20px;padding:15px;background:#f0f5ff;border-left:4px solid #646db1;font-size:13px;font-style:italic}}
</style>
</head><body><div class="container">
<img src="https://github.com/LakovskyR/IMB-certification/blob/main/header.png?raw=true" style="width:100%;display:block">
<div class="content">
<div class="intro-text">
Chère équipe,<br><br>
Centres livrés hier (SC highlighted) + état du mois.<br><br>
🌟 <strong>Centres livrés hier:</strong>
</div>
<table><thead><tr>
<th>Chainage</th><th>Catégorie</th><th>SC veille</th><th>IV veille</th>
<th>IV+SC mois</th><th># Cmd mois</th><th>1ère SC</th><th>Moy 4M</th>
</tr></thead><tbody>{rows}</tbody></table>
<div class="section-title">🎯 National</div>
<div class="kpi-container">
<div class="kpi-card"><img src="cid:kpi_chart" style="width:100%"></div>
<div class="kpi-card"><img src="cid:vol_chart" style="width:100%"></div>
</div>
<div class="chart"><img src="cid:daily_chart" style="width:100%;max-width:900px"></div>
<div class="section-title">🚀 12 derniers mois</div>
<div class="chart"><img src="cid:monthly_chart" style="width:100%;max-width:900px"></div>
<div class="signature">
Merci pour l'engagement Ocrevus SC ! 🚀<br><br>
Bien à vous,<br><strong>Nele et Diane-Laure</strong>
</div>
<div class="ps"><strong>P.S. AI</strong> {ps_content}</div>
</div></div></body></html>"""
    
    return html

def send_email(recipients, subject, html):
    """Send email via SMTP"""
    try:
        msg = MIMEMultipart('related')
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        
        msg_alt = MIMEMultipart('alternative')
        msg.attach(msg_alt)
        msg_alt.attach(MIMEText(html, 'html'))
        
        for cid, path in [
            ('kpi_chart', '/tmp/kpi_chart.png'),
            ('vol_chart', '/tmp/vol_chart.png'),
            ('daily_chart', '/tmp/daily_chart.png'),
            ('monthly_chart', '/tmp/monthly_chart.png')
        ]:
            with open(path, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-ID', f'<{cid}>')
                msg.attach(img)
        
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        
        print(f"   ✅ Sent to {len(recipients)} recipients")
    except Exception as e:
        print(f"   ❌ Error: {e}")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        # Extract
        df_raw = connect_tableau()
        
        # Transform
        df = process_data(df_raw)
        
        # Calculate
        final_table = calculate_metrics(df)
        
        # Charts
        metrics = generate_charts(final_table, df)
        
        # AI
        ps_content = generate_ai_content(metrics)
        
        # Email
        print("--- Sending Emails ---")
        
        yesterday = datetime.now() - timedelta(days=1)
        email_date = yesterday.strftime('%d/%m/%Y')
        
        if ACTIVE_GROUP == 'prod_sectorised':
            # Send to each sector
            all_sectors = final_table['secteur_promo'].dropna().unique()
            
            for sector in sorted(all_sectors):
                df_sector = final_table[final_table['secteur_promo'] == sector].copy()
                
                # Get recipients
                emails = set()
                for col in ['email_promo', 'email_medical', 'email_ma']:
                    for email in df_sector[col].dropna().unique():
                        if '@' in str(email):
                            emails.add(str(email).strip())
                
                if not emails:
                    continue
                
                sec_iv = df_sector['Volume MTT Ocrevus IV de la veille'].sum()
                sec_sc = df_sector['Volume MTT Ocrevus SC de la veille'].sum()
                subject = f"OCREVUS {email_date}. {sector}: IV={int(sec_iv)}, SC={int(sec_sc)}"
                
                html = build_html(df_sector)
                send_email(list(emails), subject, html)
                time.sleep(1)
        
        else:
            # National mode (test)
            test_recipients = [SENDER_EMAIL]
            nat_iv = final_table['Volume MTT Ocrevus IV de la veille'].sum()
            nat_sc = final_table['Volume MTT Ocrevus SC de la veille'].sum()
            subject = f"OCREVUS {email_date}. National: IV={int(nat_iv)}, SC={int(nat_sc)}"
            
            html = build_html(final_table)
            send_email(test_recipients, subject, html)
        
        print("✅ Process complete")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
