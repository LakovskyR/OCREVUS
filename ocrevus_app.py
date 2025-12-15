# -*- coding: utf-8 -*-
"""
Ocrevus Automation Script v4.1
- Pixel tracking integration
- Updated chart titles & styling (Larger titles for 1 & 2)
- Legend spacing fix for email clients
- KPI Digit moved outside chart
"""

import os
import sys
import io
import time
import smtplib
import ssl
import hashlib
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
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Missing libraries: {e}")
    sys.exit(1)

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

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
USE_AI = int(os.getenv('USE_AI', '0'))  # 0=disabled, 1=enabled

# Tracker
TRACKER_URL = os.getenv('TRACKER_URL', 'https://ocrevus-tracker.onrender.com')

# Active Group
ACTIVE_RECIPIENT_GROUP = os.getenv('ACTIVE_RECIPIENT_GROUP', 'test_1')

# Recipient Groups
RECIPIENT_GROUPS = {
    'test_1': ["roman.lakovskiy@contractors.roche.com"],
    'test_2': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "diane-laure.trouvet@roche.com"
    ],
    'test_3': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "diane-laure.trouvet@roche.com"
    ],
    'prod_national_view': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "diane-laure.trouvet@roche.com"
    ]
}

# Styling
COLORS = {'ocrevus_sc': '#ffc72a', 'ocrevus_iv': '#646db1', 'background': '#f5f5f3'}
FONT_FAMILY = 'Arial'
CHART_TITLE_SIZE = 19
CHART_TEXT_MAIN = 14
CHART_ANNOTATION = 16
CHART_TEXT_STANDARD = 13

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def generate_tracking_id(recipient_email, sector, date_str):
    """Generate unique tracking ID: ocrevus_20251215_TERR013_a3f2b1"""
    email_hash = hashlib.md5(recipient_email.encode()).hexdigest()[:6]
    sector_clean = sector.replace('_', '').replace('-', '')[:15]
    date_clean = date_str.replace('/', '')
    return f"ocrevus_{date_clean}_{sector_clean}_{email_hash}"

# =============================================================================
# DATA EXTRACTION
# =============================================================================

def fetch_tableau_data():
    print(f"--- Connecting to Tableau: {SERVER_URL} ---")
    auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_ID)
    server = TSC.Server(SERVER_URL, use_server_version=True)
    
    with server.auth.sign_in(auth):
        req = TSC.RequestOptions()
        req.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, WORKBOOK_NAME))
        workbooks, _ = server.workbooks.get(req)
        
        target_view = None
        for wb in workbooks:
            server.workbooks.populate_views(wb)
            for view in wb.views:
                if view.name == VIEW_NAME:
                    target_view = view
                    break
            if target_view: break
            
        if not target_view: raise Exception(f"View '{VIEW_NAME}' not found")
        
        print(f"✓ Downloading data from view: {VIEW_NAME}")
        server.views.populate_csv(target_view)
        csv_data = io.StringIO(b"".join(target_view.csv).decode("utf-8"))
        return pd.read_csv(csv_data)

# =============================================================================
# PROCESSING
# =============================================================================

def unpivot_data(df_raw):
    print("--- Processing Data ---")
    dim_cols = [col for col in df_raw.columns if col not in ['Measure Names', 'Measure Values']]
    df = df_raw.pivot(index=dim_cols, columns='Measure Names', values='Measure Values').reset_index()
    df.columns.name = None
    
    # Rename columns
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
    print("--- Calculating Metrics ---")
    
    today = datetime.now().date()
    yesterday_date = today - timedelta(days=3 if today.weekday() == 0 else 1)
    
    # Yesterday data
    if today.weekday() == 0:
        df_yesterday = df[(df['date_day'].dt.date >= yesterday_date) & (df['date_day'].dt.date < today)].copy()
    else:
        df_yesterday = df[df['date_day'].dt.date == yesterday_date].copy()
    
    df_table = df_yesterday.groupby(['chainage_cip', 'chainage_name']).agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_table.columns = ['chainage_cip', 'chainage_name', 'Volume MTT Ocrevus IV de la veille', 'Volume MTT Ocrevus SC de la veille']
    
    # MTD
    current_month = today.replace(day=1)
    df_mtd = df[df['date_day'].dt.date >= current_month].copy()
    df_mtd_agg = df_mtd.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum', 'center_cip': 'count'}).reset_index()
    df_mtd_agg.columns = ['chainage_cip', 'volume_iv_mtd', 'volume_sc_mtd', 'nb_orders_mtd']
    
    # 4-month avg
    start_4m = current_month - timedelta(days=120)
    df_4m = df[(df['date_day'].dt.date >= start_4m) & (df['date_day'].dt.date < current_month)].copy()
    df_4m_agg = df_4m.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_4m_agg['avg_4m'] = (df_4m_agg['volume_iv'] + df_4m_agg['volume_sc']) / 4.0
    
    # First SC order
    df_first_sc = df[df['volume_sc'] > 0].groupby('chainage_cip')['date_day'].min().reset_index()
    df_first_sc.columns = ['chainage_cip', 'date_first_sc']
    
    # Category
    cats = df.groupby('chainage_cip')['category'].first().reset_index()
    
    # Sector info
    sector_info = df[['chainage_cip', 'secteur_promo', 'secteur_medical', 'secteur_ma', 
                      'email_promo', 'email_medical', 'email_ma']].drop_duplicates('chainage_cip')
    
    # Merge all
    final = df_table.merge(df_mtd_agg, on='chainage_cip', how='left') \
                    .merge(df_4m_agg[['chainage_cip', 'avg_4m']], on='chainage_cip', how='left') \
                    .merge(df_first_sc, on='chainage_cip', how='left') \
                    .merge(cats, on='chainage_cip', how='left') \
                    .merge(sector_info, on='chainage_cip', how='left')
    
    # Format columns
    final['Volume MTT Ocrevus IV+SC dans le mois'] = final['volume_iv_mtd'].fillna(0) + final['volume_sc_mtd'].fillna(0)
    final['Nombre de commandes dans le mois d\'Ocrevus IV+SC'] = final['nb_orders_mtd'].fillna(0).astype(int)
    final['Date 1ère commande Ocrevus SC'] = final['date_first_sc'].dt.strftime('%d/%m/%Y').fillna('')
    final['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'] = final['avg_4m'].fillna(0).round(2)
    final['Catégorie de centres'] = final['category'].fillna('N/A')
    
    # Rename for display
    final = final.rename(columns={'chainage_name': 'Centres'})
    
    # Select final columns
    final = final[[
        'Centres', 'chainage_cip', 'Catégorie de centres',
        'secteur_promo', 'secteur_medical', 'secteur_ma',
        'email_promo', 'email_medical', 'email_ma',
        'Volume MTT Ocrevus SC de la veille',
        'Volume MTT Ocrevus IV de la veille',
        'Volume MTT Ocrevus IV+SC dans le mois',
        'Nombre de commandes dans le mois d\'Ocrevus IV+SC',
        'Date 1ère commande Ocrevus SC',
        'Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'
    ]]
    
    return final.fillna('')

# =============================================================================
# CHART GENERATION
# =============================================================================

def generate_charts(df_full):
    print("--- Generating Charts ---")
    
    # Current month
    current_month = datetime.now().replace(day=1).date()
    df_mtd = df_full[df_full['date_day'].dt.date >= current_month]
    iv = df_mtd['volume_iv'].sum()
    sc = df_mtd['volume_sc'].sum()
    
    # Chart 1: KPI - Centers with SC
    df_sc_centers = df_full[df_full['volume_sc'] > 0][['chainage_cip', 'category']].drop_duplicates()
    df_kpi = df_sc_centers.groupby('category').size().reset_index(name='Nombre de centres')
    df_kpi = df_kpi.rename(columns={'category': 'Catégorie'})
    
    all_cats = pd.DataFrame({'Catégorie': ['C1', 'C2', 'C3', 'C4']})
    df_kpi = all_cats.merge(df_kpi, on='Catégorie', how='left')
    df_kpi['Nombre de centres'] = df_kpi['Nombre de centres'].fillna(0).astype(int)
    total_hco = df_kpi['Nombre de centres'].sum()
    
    fig_kpi = px.bar(df_kpi, x='Catégorie', y='Nombre de centres',
                     color_discrete_sequence=[COLORS['ocrevus_sc']], text='Nombre de centres')
    
    # KPI Digit Outside to the LEFT
    # Added left margin to accommodate the digit
    fig_kpi.update_layout(
        template='plotly_white', height=500, width=700, # Match Chart 2 width
        font=dict(family=FONT_FAMILY, size=CHART_TEXT_MAIN),
        title=dict(text='Nombre de centres qui ont initié Ocrevus SC', 
                  font=dict(size=24, family=FONT_FAMILY), y=0.98, x=0.5, xanchor='center'),
        yaxis=dict(rangemode='tozero', tick0=0, dtick=1, title=None),
        xaxis=dict(title=None),
        margin=dict(l=150, b=140, t=80) # Left margin for digit
    )
    
    # Total number - LEFT ALIGNED, outside chart
    fig_kpi.add_annotation(
        text=f'<b>{total_hco}</b>', xref="paper", yref="paper",
        x=-0.25, y=0.5, showarrow=False,  # Outside left, centered vertically
        font=dict(size=60, family=FONT_FAMILY), xanchor='center'
    )
    
    # Ambition text BELOW chart
    fig_kpi.add_annotation(
        text="<i>Ambition : 70% des C1/C2 et 50% des C3 ont commandé Ocrevus SC<br>dans les 4 mois suivants le lancement soit 119 centres</i>",
        xref="paper", yref="paper", x=0.5, y=-0.28, showarrow=False,
        font=dict(size=CHART_ANNOTATION, family=FONT_FAMILY), align="center"
    )
    
    fig_kpi.update_traces(textfont=dict(size=CHART_TEXT_MAIN))
    fig_kpi.write_image('/tmp/kpi.png', scale=2)
    
    # Chart 2: Pie
    labels = ['IV', 'SC'] if sc > 0 else ['IV']
    values = [iv, sc] if sc > 0 else [iv]
    colors = [COLORS['ocrevus_iv'], COLORS['ocrevus_sc']] if sc > 0 else [COLORS['ocrevus_iv']]
    
    fig_vol = go.Figure(data=[go.Pie(
        labels=labels, values=values, marker=dict(colors=colors),
        textinfo='label+value+percent',
        texttemplate='%{label}<br>%{value:,.0f}<br>(%{percent})',
        textfont=dict(size=16, family=FONT_FAMILY)  # +1 size (was 15)
    )])
    
    fig_vol.update_layout(
        title=dict(text='Ventes Ocrevus SC / IV sur le mois en cours',
                  x=0.5, y=0.98, xanchor='center', font=dict(size=24, family=FONT_FAMILY)),  # Added y=0.98 to align with Chart 1
        template='plotly_white', height=550, width=700,  # Even bigger
        margin=dict(l=50, r=50, t=80, b=170), showlegend=False
    )
    
    fig_vol.write_image('/tmp/vol.png', scale=2)
    
    # Chart 3: Daily (FRENCH DAY LABELS)
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
            business_days.insert(0, current_day.date())
        current_day -= timedelta(days=1)
    
    df_d = df_full[df_full['date_day'].dt.date.isin(business_days)].groupby('date_day').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().sort_values('date_day')
    
    # FRENCH DAY LABELS
    day_map = {'Mon': 'Lun', 'Tue': 'Mar', 'Wed': 'Mer', 'Thu': 'Jeu', 'Fri': 'Ven'}
    df_d['day'] = df_d['date_day'].dt.strftime('%a').map(day_map)
    
    fig_d = go.Figure()
    
    fig_d.add_trace(go.Bar(
        x=df_d['day'], y=df_d['volume_iv'], name='IV',
        marker=dict(color=COLORS['ocrevus_iv']),
        text=df_d['volume_iv'].astype(int),
        textposition='inside', insidetextanchor='middle',
        textfont=dict(color='white')
    ))
    
    if df_d['volume_sc'].sum() > 0:
        fig_d.add_trace(go.Bar(
            x=df_d['day'], y=df_d['volume_sc'], name='SC',
            marker=dict(color=COLORS['ocrevus_sc']),
            text=df_d['volume_sc'].astype(int),
            textposition='inside', insidetextanchor='middle'
        ))
    
    fig_d.update_layout(
        barmode='stack', template='plotly_white', height=400, width=900,
        title=dict(text='Evolution quotidienne des volumes d\'Ocrevus IV et SC',
                  font=dict(size=18), x=0.5, xanchor='center'),  # Size 18, centered
        yaxis=dict(visible=False),
        showlegend=False
    )
    
    fig_d.write_image('/tmp/daily.png', scale=2)
    
    # Chart 4: Monthly (NO Y-AXIS)
    df_full['m'] = df_full['date_day'].dt.to_period('M')
    df_m = df_full.groupby('m').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().tail(12)
    df_m['lbl'] = df_m['m'].dt.strftime('%m/%y')
    
    fig_m = go.Figure()
    
    fig_m.add_trace(go.Bar(
        x=df_m['lbl'], y=df_m['volume_iv'], name='IV',
        marker=dict(color=COLORS['ocrevus_iv']),
        text=[f'{v/1000:.2f}K' for v in df_m['volume_iv']],
        textposition='outside'
    ))
    
    fig_m.add_trace(go.Bar(
        x=df_m['lbl'], y=df_m['volume_sc'], name='SC',
        marker=dict(color=COLORS['ocrevus_sc']),
        text=[f'{v/1000:.2f}K' if v>0 else '' for v in df_m['volume_sc']],
        textposition='outside'
    ))
    
    fig_m.update_layout(
        barmode='stack', template='plotly_white', height=400, width=900,
        title=dict(text='Evolution mensuelle des volumes d\'Ocrevus IV et SC',
                  font=dict(size=18), x=0.5, xanchor='center'),  # Size 18, centered
        yaxis=dict(visible=False, range=[0, max(df_m['volume_iv'] + df_m['volume_sc']) * 1.4]),
        showlegend=False
    )
    
    fig_m.write_image('/tmp/monthly.png', scale=2)
    
    return int(iv), int(sc), total_hco

# =============================================================================
# EMAIL GENERATION
# =============================================================================

def get_ai_content(iv, sc, total_centers, target_total=4686, sector_name=None, sector_iv=None, sector_sc=None):
    """Generate AI content if USE_AI=1"""
    if USE_AI != 1:
        return None  # Skip AI
    
    try:
        client = Perplexity(api_key=PPLX_API_KEY)
        
        today = datetime.now()
        first_day = today.replace(day=1)
        last_day = (first_day.replace(month=first_day.month % 12 + 1, year=first_day.year if first_day.month < 12 else first_day.year + 1) - timedelta(days=1))
        
        total_working_days = 0
        days_passed = 0
        
        for d in range(last_day.day):
            day_date = first_day + timedelta(days=d)
            if day_date.weekday() < 5:
                total_working_days += 1
                if day_date <= today:
                    days_passed += 1
                    
        pct_month_passed = (days_passed / total_working_days * 100) if total_working_days > 0 else 0
        total_sales = iv + sc
        pct_target_reached = (total_sales / target_total * 100) if target_total > 0 else 0
        
        sector_context = ""
        if sector_name and sector_iv is not None:
             sector_context = f"\nFOCUS SECTEUR ({sector_name}): Performance locale: {sector_iv} IV + {sector_sc} SC."

        prompt = f"""Contexte Ocrevus (SEP):
- IV: traitement existant
- SC: nouveau lancement (12/12/2024), phase d'attente des premières commandes.

Situation Nationale au {today.strftime('%d/%m/%Y')}:
- Ventes Totales: {total_sales} (IV: {iv}, SC: {sc}) sur objectif {target_total}.
- Progression: {pct_target_reached:.0f}% de l'objectif atteint en {pct_month_passed:.0f}% des jours ouvrés du mois.
- Centres SC activés: {total_centers}.
{sector_context}

Instruction: Rédige un court paragraphe (2-3 phrases) en français sur un ton très optimiste et encourageant.
1. Souligne la bonne dynamique globale par rapport à l'avancement du mois (ex: "X% de l'objectif en Y% du temps").
2. Mentionne l'attente confiante des commandes SC.
3. Si un focus secteur est fourni ci-dessus, inclus un mot rapide et positif sur leur contribution spécifique.
IMPORTANT: Ne PAS inclure de références ou de citations comme [1], [2]."""
        
        resp = client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model="sonar")
        content = resp.choices[0].message.content
        
        for i in range(1, 10):
            content = content.replace(f'[{i}]', '')
        return content.replace('**','').replace('*','')
    except Exception as e:
        print(f"AI Error: {e}")
        return "Le rythme global est excellent. Nous attendons avec confiance les premières commandes SC pour compléter cette belle dynamique."

def build_html_v4(table_df, ps_content=None, tracking_id=None):
    """Build HTML with updated styling and optional tracking pixel"""
    
    df_sorted = table_df.sort_values('Volume MTT Ocrevus IV+SC dans le mois', ascending=False)
    
    rows = ""
    for _, row in df_sorted.iterrows():
        rows += f"""
        <tr>
            <td style="font-size: 11px; color: #000;">{row['Centres']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Catégorie de centres']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus SC de la veille']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus IV de la veille']}</td>
            <td style="text-align: center; font-weight: bold; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus IV+SC dans le mois']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row["Nombre de commandes dans le mois d'Ocrevus IV+SC"]}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Date 1ère commande Ocrevus SC']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois']}</td>
        </tr>"""
    
    # Build PS section only if ps_content exists
    ps_section = ""
    if ps_content:
        ps_section = f'<div class="ps"><strong>P.S. AI</strong> {ps_content}</div>'
    
    # Build tracking pixel if tracking_id exists
    tracking_pixel = ""
    if tracking_id:
        tracking_pixel = f'<img src="{TRACKER_URL}/pixel/{tracking_id}.png" width="1" height="1" style="display:none" alt="">'

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background-color: #f5f5f3; }}
        .container {{ max-width: 900px; margin: 0 auto; background-color: white; }}
        .content {{ padding: 20px 40px; }}
        .intro-text {{ font-size: 14px; line-height: 1.8; margin-bottom: 20px; color: #000; }}
        .section-title {{ font-size: 18px; font-weight: bold; margin: 30px 0 15px 0; color: #000; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 11px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        th {{ background-color: #252b5c; color: white; padding: 12px 8px; text-align: center; font-weight: bold; border: 1px solid #1a1f3d; font-size: 11px; line-height: 1.3; min-width: 80px; }}
        th.sc-header {{ background-color: #ffc72a; color: #000; }}
        th.iv-header {{ background-color: #646db1; color: white; }}
        td {{ padding: 10px 8px; border: 1px solid #e0e0e0; color: #000; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f0f0f0; }}
        /* UPDATED LEGEND STYLING */
        .legend {{ display: flex; justify-content: center; margin: 20px 0; font-size: 16px; font-weight: bold; }}
        .legend-item {{ display: flex; align-items: center; margin: 0 30px; }}
        .legend-box {{ width: 30px; height: 20px; border-radius: 4px; margin-right: 15px; }}
        .separator {{ height: 2px; background: #e0e0e0; margin: 30px 0; }}
        .vertical-separator {{ width: 2px; background: #e0e0e0; }}
        .kpi-container {{ display: flex; justify-content: space-between; margin: 20px 0; gap: 20px; }}
        .kpi-card {{ flex: 1; }}
        .chart {{ text-align: center; margin: 20px 0; }}
        .signature {{ margin-top: 30px; font-size: 14px; line-height: 1.8; color: #000; }}
        .ps {{ margin-top: 20px; padding: 15px; background-color: #f0f5ff; border-left: 4px solid #646db1; font-size: 13px; font-style: italic; color: #000; }}
        a {{ color: #0066cc; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <img src="https://github.com/LakovskyR/IMB-certification/blob/main/header.png?raw=true" alt="Header" style="width: 100%; display: block;">
        <div class="content">
            <div class="intro-text">
                Chère équipe,<br><br>
                Veuillez trouver ci-après :<br>
                -  les centres qui ont été livrés de l'Ocrevus la veille<br>
                - un focus sur la performance Ocrevus SC<br>
                - un état d'avancement  de où est ce qu'on en est dans le mois<br><br>
                N'hésitez pas à compléter ces informations avec celles présentes dans le <a href="https://eu-west-1a.online.tableau.com/#/site/tabemeacloud/views/DashboardNeurologie/Ventesinternes?:iid=1">dashboard neuro</a> et <a href="https://customer.roche.com/customer-focus">CES</a> !<br><br>
                🌟 <strong>Les centres qui ont reçu de l'Ocrevus la veille :</strong>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Centres</th>
                        <th>Catégorie<br>de centres</th>
                        <th class="sc-header">Volume MTT<br>Ocrevus SC<br>de la veille</th>
                        <th class="iv-header">Volume MTT<br>Ocrevus IV<br>de la veille</th>
                        <th>Volume MTT<br>Ocrevus IV+SC<br>dans le mois</th>
                        <th>Nombre de<br>commandes<br>dans le mois<br>d'Ocrevus IV+SC</th>
                        <th>Date 1ère<br>commande<br>Ocrevus SC</th>
                        <th>AVG IV+SC CM4</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            
            <div class="section-title">🎯 Où ce qu'on en est au niveau national, à date ?</div>
            
            <div class="legend">
                <div class="legend-item">
                    <div class="legend-box" style="background-color: #646db1;"></div>
                    <span>Ocrevus IV</span>
                </div>
                <div class="legend-item">
                    <div class="legend-box" style="background-color: #ffc72a;"></div>
                    <span>Ocrevus SC</span>
                </div>
            </div>
            
            <div class="kpi-container">
                <div class="kpi-card"><img src="cid:kpi_chart" style="width: 100%; border-radius: 4px;"></div>
                <div class="vertical-separator"></div>
                <div class="kpi-card"><img src="cid:vol_chart" style="width: 100%; border-radius: 4px;"></div>
            </div>
            
            <div class="separator"></div>
            
            <div class="chart"><img src="cid:daily_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            
            <div class="separator"></div>
            
            <div class="section-title">🚀 Et où ce qu'on en est sur les 12 derniers mois ?</div>
            <div class="chart"><img src="cid:monthly_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            
            <div class="signature">
                Merci à tous pour l'engagement que vous avez autour d'Ocrevus SC ! Keep going<br><br>
                Bien à vous,<br>
                <strong>Nele et Diane-Laure</strong>
            </div>
            {ps_section}
            {tracking_pixel}
        </div>
    </div>
</body>
</html>"""

def send_email(recipients, subject, html_content):
    if not recipients: return
    try:
        msg = MIMEMultipart('related')
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        msg_alt = MIMEMultipart('alternative')
        msg.attach(msg_alt)
        msg_alt.attach(MIMEText(html_content, 'html'))
        
        for cid, path in [('kpi_chart', '/tmp/kpi.png'), ('vol_chart', '/tmp/vol.png'),
                          ('daily_chart', '/tmp/daily.png'), ('monthly_chart', '/tmp/monthly.png')]:
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', f'<{cid}>')
                    img.add_header('Content-Disposition', 'inline', filename=path)
                    msg.attach(img)
                    
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        print(f"   ✅ Sent to {len(recipients)} recipients")
    except Exception as e:
        print(f"   ❌ Error: {e}")

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    try:
        print(f"--- Starting Ocrevus Report ({ACTIVE_RECIPIENT_GROUP}, AI={'ON' if USE_AI else 'OFF'}) ---")
        
        # Extract
        df_raw = fetch_tableau_data()
        
        # Transform
        df = unpivot_data(df_raw)
        
        # Calculate
        final_table = calculate_metrics(df)
        
        # Charts
        vol_iv, vol_sc, total_centers = generate_charts(df)
        
        # Date
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%d/%m/%Y')
        
        # National metrics
        nat_iv = int(final_table['Volume MTT Ocrevus IV de la veille'].sum())
        nat_sc = int(final_table['Volume MTT Ocrevus SC de la veille'].sum())
        
        print("--- Sending Emails ---")
        
        # PROD SECTORISED MODE
        if ACTIVE_RECIPIENT_GROUP == 'prod_sectorised':
            print("Running prod_sectorised mode...")
            
            # Loop 1: PROMO sectors
            promo_sectors = final_table['secteur_promo'].dropna().unique()
            for sector in sorted(promo_sectors):
                df_sec = final_table[final_table['secteur_promo'] == sector].copy()
                
                # Get unique PROMO emails for this sector
                recipients = []
                for mail in df_sec['email_promo'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers, 
                                               sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory {sector}: IV={sec_iv}, SC={sec_sc}"
                    
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Loop 2: MEDICAL sectors
            med_sectors = final_table['secteur_medical'].dropna().unique()
            for sector in sorted(med_sectors):
                df_sec = final_table[final_table['secteur_medical'] == sector].copy()
                
                recipients = []
                for mail in df_sec['email_medical'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers,
                                               sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory {sector}: IV={sec_iv}, SC={sec_sc}"
                    
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Loop 3: MA sectors
            ma_sectors = final_table['secteur_ma'].dropna().unique()
            for sector in sorted(ma_sectors):
                df_sec = final_table[final_table['secteur_ma'] == sector].copy()
                
                recipients = []
                for mail in df_sec['email_ma'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers,
                                               sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory {sector}: IV={sec_iv}, SC={sec_sc}"
                    
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Final: National view to managers
            print("Sending National View to Managers...")
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers)
            subject_nat = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}"
            tracking_id = generate_tracking_id(RECIPIENT_GROUPS['prod_national_view'][0], 'NATIONAL', date_str)
            html_nat = build_html_v4(final_table, ps_content, tracking_id)
            send_email(RECIPIENT_GROUPS['prod_national_view'], subject_nat, html_nat)
        
        elif ACTIVE_RECIPIENT_GROUP == 'test_3':
            # Test sectorised - one sector
            print("Running test_3 (sectorised test)...")
            
            active_sectors = final_table[
                (final_table['Volume MTT Ocrevus IV de la veille'] > 0) | 
                (final_table['Volume MTT Ocrevus SC de la veille'] > 0)
            ]['secteur_promo'].unique()
            
            target_sector = active_sectors[0] if len(active_sectors) > 0 else final_table['secteur_promo'].unique()[0]
            
            df_sec = final_table[final_table['secteur_promo'] == target_sector].copy()
            sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
            sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
            
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers,
                                       sector_name=target_sector, sector_iv=sec_iv, sector_sc=sec_sc)
            
            subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory {target_sector}: IV={sec_iv}, SC={sec_sc}"
            
            tracking_id = generate_tracking_id(RECIPIENT_GROUPS['test_3'][0], target_sector, date_str)
            html = build_html_v4(df_sec, ps_content, tracking_id)
            send_email(RECIPIENT_GROUPS['test_3'], subject, html)
        
        else:
            # National mode (test_1, test_2, prod)
            print(f"Running {ACTIVE_RECIPIENT_GROUP} (national view)...")
            recipients = RECIPIENT_GROUPS.get(ACTIVE_RECIPIENT_GROUP, [SENDER_EMAIL])
            subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}"
            
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers)
            
            tracking_id = generate_tracking_id(recipients[0], 'NATIONAL', date_str)
            html = build_html_v4(final_table, ps_content, tracking_id)
            send_email(recipients, subject, html)
        
        print("✅ Process Complete")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)