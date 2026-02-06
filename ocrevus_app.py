# -*- coding: utf-8 -*-
"""
Ocrevus Automation Script v5.2
- FIX: Pie chart now handles empty data (start of month) gracefully
- FIX: Pie chart pull parameter is dynamic to prevent length mismatch errors
- CHANGE: Removed disclaimer text about forecast
"""

import os
import sys
import io
import re
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

# CSV Mailing List
CSV_MAIL_LIST_URL = 'https://raw.githubusercontent.com/LakovskyR/OCREVUS/main/mail_list.csv'

# Active Group
ACTIVE_RECIPIENT_GROUP = os.getenv('ACTIVE_RECIPIENT_GROUP', 'test_1')

# Recipient Groups
RECIPIENT_GROUPS = {
    'test_1': ["roman.lakovskiy@contractors.roche.com"],
    'test_2': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "timothee.picot@roche.com",
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
    ],
    'prod_csv': []  # Will be loaded from CSV file
}

# Styling
COLORS = {'ocrevus_sc': '#ffc72a', 'ocrevus_iv': '#646db1', 'background': '#f5f5f3'}
FONT_FAMILY = 'Roche Sans, Arial, sans-serif'
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

def load_emails_from_csv(csv_url):
    """Load email addresses from CSV file (GitHub or local)"""
    try:
        import urllib.request
        print(f"   Loading emails from CSV: {csv_url}")
        
        # Try to fetch from URL
        try:
            response = urllib.request.urlopen(csv_url)
            csv_content = response.read().decode('utf-8')
        except:
            # If URL fails, try local file
            csv_path = csv_url.split('/')[-1]
            if os.path.exists(csv_path):
                with open(csv_path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
            else:
                print(f"   ⚠ Could not load CSV from URL or local file")
                return []
        
        # Parse emails from CSV
        emails = []
        lines = csv_content.strip().split('\n')
        
        for line in lines[1:]:  # Skip header
            line = line.strip()
            if not line:
                continue
            
            email_match = re.search(r'<([^>]+)>', line)
            if email_match:
                email = email_match.group(1).strip()
            else:
                email = line.strip()
            
            if '@' in email and '.' in email:
                emails.append(email)
        
        print(f"   ✓ Loaded {len(emails)} emails from CSV")
        return emails
        
    except Exception as e:
        print(f"   ❌ Error loading CSV: {e}")
        return []

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

def fetch_tableau_view(view_name):
    """Fetch data from a specific Tableau view"""
    print(f"--- Fetching additional view: {view_name} ---")
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
                if view.name == view_name:
                    target_view = view
                    break
            if target_view: break
            
        if not target_view: 
            print(f"⚠ View '{view_name}' not found")
            return None
        
        print(f"✓ Downloading data from view: {view_name}")
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
    
    # Convert volumes to numeric (handles strings from Tableau with commas)
    # SQL already divides IV by 2, so we just convert to numeric here
    if df['volume_iv'].dtype == 'object':
        df['volume_iv'] = df['volume_iv'].astype(str).str.replace(',', '.')
    if df['volume_sc'].dtype == 'object':
        df['volume_sc'] = df['volume_sc'].astype(str).str.replace(',', '.')
        
    df['volume_iv'] = pd.to_numeric(df['volume_iv'], errors='coerce').fillna(0)
    df['volume_sc'] = pd.to_numeric(df['volume_sc'], errors='coerce').fillna(0)
    
    return df

def calculate_metrics(df):
    print("--- Calculating Metrics ---")
    
    latest_date_in_data = df['date_day'].max().date()
    system_today = datetime.now().date()
    
    # ALWAYS use yesterday's date for query_date
    query_date = system_today - timedelta(days=1)
    
    # FIX: Handle Monday correctly - check if TODAY is Monday
    # If TODAY is Monday, aggregate Fri+Sat+Sun (weekend period)
    # Otherwise use query_date data directly
    if system_today.weekday() == 0:  # Monday
        friday = query_date - timedelta(days=2)
        df_yesterday = df[(df['date_day'].dt.date >= friday) & (df['date_day'].dt.date <= query_date)].copy()
        print(f"   📅 Monday detected: Aggregating Fri-Sun ({friday} to {query_date})")
    else:
        df_yesterday = df[df['date_day'].dt.date == query_date].copy()
        print(f"   📅 Using data for: {query_date}")
    
    df_table = df_yesterday.groupby(['chainage_cip', 'chainage_name']).agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_table.columns = ['chainage_cip', 'chainage_name', "Volume MTT Ocrevus IV d'hier", "Volume MTT Ocrevus SC d'hier"]
    
    # MTD
    current_month = query_date.replace(day=1)
    df_mtd = df[df['date_day'].dt.date >= current_month].copy()
    df_mtd_agg = df_mtd.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum', 'center_cip': 'count'}).reset_index()
    df_mtd_agg.columns = ['chainage_cip', 'volume_iv_mtd', 'volume_sc_mtd', 'nb_orders_mtd']
    
    # 4-month avg (last 4 COMPLETED months, excluding current month)
    temp_date = current_month
    for _ in range(4):
        temp_date = (temp_date - timedelta(days=1)).replace(day=1)
    start_4m = temp_date
    
    df_4m = df[(df['date_day'].dt.date >= start_4m) & (df['date_day'].dt.date < current_month)].copy()
    df_4m_agg = df_4m.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_4m_agg['avg_4m'] = (df_4m_agg['volume_iv'] + df_4m_agg['volume_sc']) / 4.0
    
    # First SC order
    df_first_sc = df[df['volume_sc'] > 0].groupby('chainage_cip')['date_day'].min().reset_index()
    df_first_sc.columns = ['chainage_cip', 'date_first_facturation_sc']
    
    # Category
    cats = df.sort_values('date_day', ascending=False).groupby('chainage_cip')['category'].first().reset_index()
    
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
    final['Date 1ère facturation Ocrevus SC'] = final['date_first_facturation_sc'].dt.strftime('%d/%m/%Y').fillna('')
    final['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'] = final['avg_4m'].fillna(0).round(2)
    final['Catégorie de centres'] = final['category'].fillna('N/A')
    
    final = final.rename(columns={'chainage_name': 'Centres'})
    
    final = final[[
        'Centres', 'chainage_cip', 'Catégorie de centres',
        'secteur_promo', 'secteur_medical', 'secteur_ma',
        'email_promo', 'email_medical', 'email_ma',
        "Volume MTT Ocrevus SC d'hier",
        "Volume MTT Ocrevus IV d'hier",
        'Volume MTT Ocrevus IV+SC dans le mois',
        'Nombre de commandes dans le mois d\'Ocrevus IV+SC',
        'Date 1ère facturation Ocrevus SC',
        'Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'
    ]]
    
    return final.fillna(''), query_date

# =============================================================================
# CHART GENERATION
# =============================================================================

def generate_charts(df_full, query_date, df_rated_centers=None):
    print("--- Generating Charts ---")
    
    # Debug: Show date range in data
    if not df_full.empty:
        min_date = df_full['date_day'].min().date()
        max_date = df_full['date_day'].max().date()
        print(f"   📊 Data range: {min_date} to {max_date}")
    
    current_month = query_date.replace(day=1)
    print(f"   📅 MTD filter: {current_month} to {query_date}")
    df_mtd = df_full[(df_full['date_day'].dt.date >= current_month) & (df_full['date_day'].dt.date <= query_date)]
    iv = df_mtd['volume_iv'].sum()
    sc = df_mtd['volume_sc'].sum()
    print(f"   📈 MTD volumes: IV={iv}, SC={sc}, Total={iv+sc}")
    
    # Chart 1: KPI
    chainages_with_sc = df_full[df_full['volume_sc'] > 0]['chainage_cip'].unique()
    df_recent_ratings = df_full.sort_values('date_day', ascending=False).drop_duplicates('chainage_cip')[['chainage_cip', 'category']]
    df_sc_centers = df_recent_ratings[df_recent_ratings['chainage_cip'].isin(chainages_with_sc)].copy()
    df_sc_centers['category'] = df_sc_centers['category'].replace('C4', 'Autres')
    
    df_kpi = df_sc_centers.groupby('category').size().reset_index(name='centres_with_sc')
    df_kpi = df_kpi.rename(columns={'category': 'Catégorie'})
    
    if df_rated_centers is not None:
        df_totals = df_rated_centers.copy()
        df_totals.columns = ['Catégorie', 'total_centres']
        df_totals['total_centres'] = pd.to_numeric(df_totals['total_centres'], errors='coerce').fillna(0).astype(int)
        
        df_kpi = df_totals.merge(df_kpi, on='Catégorie', how='left')
        df_kpi['centres_with_sc'] = df_kpi['centres_with_sc'].fillna(0).astype(int)
        df_kpi['percentage'] = (df_kpi['centres_with_sc'] / df_kpi['total_centres'] * 100).round(1)
        
        autres_cats = ['Autres', 'DROM COM', 'OoT']
        if set(autres_cats).issubset(set(df_kpi['Catégorie'])):
            autres_total = df_kpi[df_kpi['Catégorie'].isin(autres_cats)]['total_centres'].sum()
            autres_sc = df_kpi[df_kpi['Catégorie'].isin(autres_cats)]['centres_with_sc'].sum()
            autres_pct = (autres_sc / autres_total * 100).round(1) if autres_total > 0 else 0
            df_kpi.loc[df_kpi['Catégorie'] == 'Autres', 'percentage'] = autres_pct
            df_kpi.loc[df_kpi['Catégorie'] == 'Autres', 'centres_with_sc'] = autres_sc
            df_kpi.loc[df_kpi['Catégorie'] == 'Autres', 'total_centres'] = autres_total
            df_kpi = df_kpi[~df_kpi['Catégorie'].isin(['DROM COM', 'OoT'])]
    else:
        all_cats = pd.DataFrame({'Catégorie': ['C1', 'C2', 'C3', 'Autres']})
        df_kpi = all_cats.merge(df_kpi, on='Catégorie', how='left')
        df_kpi['centres_with_sc'] = df_kpi['centres_with_sc'].fillna(0).astype(int)
        df_kpi['percentage'] = df_kpi['centres_with_sc']
    
    total_hco = df_kpi['centres_with_sc'].sum()
    
    # Force X-axis order: C1, C2, C3, Autres
    category_order = ['C1', 'C2', 'C3', 'Autres']
    df_kpi['Catégorie'] = pd.Categorical(df_kpi['Catégorie'], categories=category_order, ordered=True)
    df_kpi = df_kpi.sort_values('Catégorie')
    
    fig_kpi = px.bar(df_kpi, x='Catégorie', y='percentage',
                     color_discrete_sequence=[COLORS['ocrevus_sc']], text='percentage',
                     category_orders={'Catégorie': category_order})
    
    fig_kpi.update_layout(
        template='plotly_white', height=450, width=600,
        font=dict(family=FONT_FAMILY, size=CHART_TEXT_MAIN),
        title=dict(text='% de centres qui ont initié Ocrevus SC par catégorie', 
                  font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY), y=0.98, x=0.5, xanchor='center'),
        yaxis=dict(visible=False), xaxis=dict(title=None),
        margin=dict(l=150, b=120, t=80) 
    )
    
    # Update x-axis labels with center counts
    if 'total_centres' in df_kpi.columns:
        ticktext = [f"{cat}({int(total)})" for cat, total in zip(df_kpi['Catégorie'], df_kpi['total_centres'])]
        fig_kpi.update_xaxes(ticktext=ticktext, tickvals=df_kpi['Catégorie'])
    
    fig_kpi.add_annotation(
        text=f'<b>{total_hco}</b>', xref="paper", yref="paper",
        x=-0.25, y=0.5, showarrow=False,
        font=dict(size=50, family=FONT_FAMILY), xanchor='center'
    )
    
    # Add percentage labels inside bars
    fig_kpi.update_traces(texttemplate='%{text}%', textfont=dict(size=CHART_TEXT_MAIN), textposition='inside', insidetextanchor='middle')
    
    # FIX #1: Add center count labels above bars
    for i, row in df_kpi.iterrows():
        fig_kpi.add_annotation(
            x=row['Catégorie'],
            y=row['percentage'],
            text=f"<b>{int(row['centres_with_sc'])}</b>",
            showarrow=False,
            yshift=15,
            font=dict(size=13, family=FONT_FAMILY, color='#000')
        )
    
    fig_kpi.write_image('/tmp/kpi.png', scale=2)
    
    # Chart 2: Pie
    iv_rounded = round(iv)
    sc_rounded = round(sc)
    total_vol = iv_rounded + sc_rounded
    
    # Handle empty MTD case - fall back to yesterday's data
    if total_vol == 0:
        # Get yesterday's data as fallback
        df_yesterday = df_full[df_full['date_day'].dt.date == query_date]
        iv_yest = df_yesterday['volume_iv'].sum()
        sc_yest = df_yesterday['volume_sc'].sum()
        iv_rounded = round(iv_yest)
        sc_rounded = round(sc_yest)
        total_vol = iv_rounded + sc_rounded
        print(f"   ⚠️ No MTD data, using yesterday's data: IV={iv_rounded}, SC={sc_rounded}")
    
    # Handle case where even yesterday has no data
    if total_vol == 0:
        labels = ['Pas de ventes']
        values = [1]  # Dummy value to render a full circle
        colors = ['#eeeeee']
        text_template = 'Pas de ventes<br>ce mois-ci'
        hover_info = 'skip'
        label_position = 'inside'
        pull_config = [0]
    else:
        labels = ['IV', 'SC'] if sc_rounded > 0 else ['IV']
        values = [iv_rounded, sc_rounded] if sc_rounded > 0 else [iv_rounded]
        colors = [COLORS['ocrevus_iv'], COLORS['ocrevus_sc']] if sc_rounded > 0 else [COLORS['ocrevus_iv']]
        text_template = '%{label}<br>%{value:,.0f}<br>(%{percent:.1%})'
        hover_info = 'label+value+percent'
        
        # Smart label positioning
        if sc_rounded > 0:
            sc_percentage = (sc_rounded / total_vol * 100)
            label_position = 'outside' if sc_percentage <= 10 else 'inside'
        else:
            label_position = 'inside'
            
        pull_config = [0] * len(labels)
    
    fig_vol = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        marker=dict(colors=colors),
        textinfo='label+value+percent' if total_vol > 0 else 'text',
        texttemplate=text_template,
        textfont=dict(size=13, family=FONT_FAMILY),
        textposition=label_position,
        pull=pull_config,
        direction='clockwise',
        sort=False,
        rotation=225,
        hoverinfo=hover_info
    )])
    
    fig_vol.update_layout(
        title=dict(
            text='Ventes Ocrevus SC / IV sur le mois en cours',
            x=0.5,
            y=0.95,
            xanchor='center',
            font=dict(size=CHART_TITLE_SIZE, family=FONT_FAMILY)
        ),
        template='plotly_white',
        height=450,
        width=500,
        margin=dict(l=40, r=40, t=60, b=40),
        showlegend=False
    )

    fig_vol.write_image('/tmp/vol.png', scale=2)
    
    # Chart 3: Daily (last 30 days including query_date)
    last_30_days = [(query_date - timedelta(days=i)) for i in range(29, -1, -1)]
    
    # FIX: Clip negative values to 0 BEFORE aggregation
    df_d_filtered = df_full[df_full['date_day'].dt.date.isin(last_30_days)].copy()
    df_d_filtered['volume_iv'] = df_d_filtered['volume_iv'].clip(lower=0)
    df_d_filtered['volume_sc'] = df_d_filtered['volume_sc'].clip(lower=0)
    
    # Now group by date
    df_d = df_d_filtered.groupby('date_day').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().sort_values('date_day')
    df_d['day_label'] = df_d['date_day'].dt.strftime('%d/%m')
    
    fig_d = go.Figure()
    fig_d.add_trace(go.Bar(x=df_d['day_label'], y=df_d['volume_iv'], name='IV', marker=dict(color=COLORS['ocrevus_iv']), text=df_d['volume_iv'].round().astype(int), textposition='inside', textfont=dict(color='white', size=10), insidetextanchor='start', cliponaxis=False))
    if df_d['volume_sc'].sum() > 0:
        sc_labels = [str(round(v)) if v > 0 else '' for v in df_d['volume_sc']]
        fig_d.add_trace(go.Bar(x=df_d['day_label'], y=df_d['volume_sc'], name='SC', marker=dict(color=COLORS['ocrevus_sc']), text=sc_labels, textposition='outside', textfont=dict(size=10), textangle=0, cliponaxis=False))
    
    fig_d.update_layout(
        barmode='stack', 
        template='plotly_white', 
        height=350, 
        width=800, 
        title=dict(text='Evolution quotidienne des volumes d\'Ocrevus IV et SC', font=dict(size=CHART_TITLE_SIZE), x=0.5, xanchor='center'), 
        yaxis=dict(visible=False), 
        xaxis=dict(tickangle=-45), 
        showlegend=False
    )
    
    fig_d.write_image('/tmp/daily.png', scale=2)
    
    # Chart 4: Monthly (13 months to compare current month vs same month last year)
    df_full_filtered = df_full[df_full['date_day'].dt.date <= query_date]
    df_full_filtered['m'] = df_full_filtered['date_day'].dt.to_period('M')
    df_m = df_full_filtered.groupby('m').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().tail(13)
    df_m['lbl'] = df_m['m'].dt.strftime('%m/%y')
    
    fig_m = go.Figure()
    fig_m.add_trace(go.Bar(x=df_m['lbl'], y=df_m['volume_iv'], name='IV', marker=dict(color=COLORS['ocrevus_iv']), text=[f'{round(v):,}'.replace(',', ' ') for v in df_m['volume_iv']], textposition='inside', textfont=dict(color='white', size=12), insidetextanchor='start'))
    fig_m.add_trace(go.Bar(x=df_m['lbl'], y=df_m['volume_sc'], name='SC', marker=dict(color=COLORS['ocrevus_sc']), text=[f'{round(v):,}'.replace(',', ' ') if v>0 else '' for v in df_m['volume_sc']], textposition='outside', textfont=dict(size=12)))
    
    fig_m.update_layout(barmode='stack', template='plotly_white', height=350, width=800, title=dict(text='Evolution mensuelle des volumes d\'Ocrevus IV et SC', font=dict(size=CHART_TITLE_SIZE), x=0.5, xanchor='center'), yaxis=dict(visible=False, range=[0, max(df_m['volume_iv'] + df_m['volume_sc']) * 1.5]), showlegend=False)
    fig_m.write_image('/tmp/monthly.png', scale=2)
    
    return int(iv), int(sc), total_hco

# =============================================================================
# EMAIL GENERATION
# =============================================================================

def generate_ambition_text(df_ambitions, reference_date):
    """Generate dynamic ambition text from CSV data"""
    if df_ambitions is None or df_ambitions.empty:
        print("   ⚠ df_ambitions is None or empty, using hardcoded values")
        return "ambition janvier : volumes Ocrevus IV : 2253 / volumes Ocrevus SC : 365 / Split SC/IV : 16%"
    
    try:
        # Get month name in French
        month_names = {
            1: 'janvier', 2: 'février', 3: 'mars', 4: 'avril',
            5: 'mai', 6: 'juin', 7: 'juillet', 8: 'août',
            9: 'septembre', 10: 'octobre', 11: 'novembre', 12: 'décembre'
        }
        month_name = month_names.get(reference_date.month, 'janvier')
        
        # Parse EndMonth column
        df_ambitions['EndMonth'] = pd.to_datetime(df_ambitions['EndMonth'], format='%m/%d/%Y', errors='coerce')
        
        # Filter for current month (match by month and year)
        current_month_data = df_ambitions[
            (df_ambitions['EndMonth'].dt.month == reference_date.month) & 
            (df_ambitions['EndMonth'].dt.year == reference_date.year)
        ]
        
        if current_month_data.empty:
            print(f"   ⚠ No ambition data found for {month_name} {reference_date.year}")
            return "ambition janvier : volumes Ocrevus IV : 2253 / volumes Ocrevus SC : 365 / Split SC/IV : 16%"
        
        # Get IV and SC values
        iv_ambition = int(current_month_data['IV'].iloc[0])
        sc_ambition = int(current_month_data['SC'].iloc[0])
        
        # Calculate split: SC / IV (not SC / Total)
        split_pct = int(round(sc_ambition / iv_ambition * 100)) if iv_ambition > 0 else 0
        
        result = f"ambition {month_name} : volumes Ocrevus IV : {iv_ambition} / volumes Ocrevus SC : {sc_ambition} / Split SC/IV : {split_pct}%"
        print(f"   ✓ Generated: {result}")
        return result
        
    except Exception as e:
        print(f"   ⚠ Error generating ambition text: {e}")
        import traceback
        traceback.print_exc()
        return "ambition janvier : volumes Ocrevus IV : 2253 / volumes Ocrevus SC : 365 / Split SC/IV : 16%"

def get_ai_content(iv, sc, total_centers, target_total=4686, sector_name=None, sector_iv=None, sector_sc=None):
    if USE_AI != 1: return None
    try:
        client = Perplexity(api_key=PPLX_API_KEY)
        today = datetime.now()
        prompt = f"""Contexte Ocrevus (SEP): IV existant, SC lancé 12/12/2024.
Situation {today.strftime('%d/%m/%Y')}: Ventes Totales: {iv+sc} (IV: {iv}, SC: {sc}) sur objectif {target_total}.
Instruction: Rédige un court paragraphe (2-3 phrases) en français sur un ton très optimiste et encourageant."""
        resp = client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model="sonar")
        return resp.choices[0].message.content.replace('**','').replace('*','')
    except: return "Le rythme global est excellent. Nous attendons avec confiance les premières commandes SC pour compléter cette belle dynamique."

def build_html_v4(table_df, ps_content=None, tracking_id=None, ambition_text=None):
    col_sc = "Volume MTT Ocrevus SC d'hier"
    col_iv = "Volume MTT Ocrevus IV d'hier"
    is_empty = table_df.empty or (table_df[col_sc].sum() == 0 and table_df[col_iv].sum() == 0)
    
    rating_order = {'C1': 1, 'C2': 2, 'C3': 3, 'Autres': 4, 'DROM COM': 5, 'OoT': 6}
    table_df['rating_sort'] = table_df['Catégorie de centres'].map(rating_order).fillna(99)
    df_sorted = table_df.sort_values(by=['rating_sort', 'Volume MTT Ocrevus IV+SC dans le mois'], ascending=[True, False])
    
    rows = ""
    for _, row in df_sorted.iterrows():
        sc_bg = "background-color: #ffffe0;" if row[col_sc] > 0 else ""
        
        def fmt(val):
            if pd.isna(val) or val == 0: return "0"
            if float(val).is_integer(): return str(int(val))
            return f"{val:.1f}".replace('.', ',')

        vol_sc = fmt(row[col_sc])
        vol_iv = fmt(row[col_iv])
        vol_mtd = fmt(row["Volume MTT Ocrevus IV+SC dans le mois"])
        nb_cmd = row["Nombre de commandes dans le mois d'Ocrevus IV+SC"]
        date_sc = row["Date 1ère facturation Ocrevus SC"]
        avg_4m = fmt(row["Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois"])

        rows += f"""<tr>
            <td style="font-size: 11px; color: #000;">{row["Centres"]}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row["Catégorie de centres"]}</td>
            <td style="text-align: center; font-size: 11px; color: #000; {sc_bg}">{vol_sc}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{vol_iv}</td>
            <td style="text-align: center; font-weight: bold; font-size: 11px; color: #000;">{vol_mtd}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{nb_cmd}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{date_sc}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{avg_4m}</td>
        </tr>"""
    
    # Add TOTAL row (only SC and IV columns have totals)
    total_sc = table_df[col_sc].sum()
    total_iv = table_df[col_iv].sum()
    
    def fmt(val):
        if pd.isna(val) or val == 0: return "0"
        if float(val).is_integer(): return str(int(val))
        return f"{val:.1f}".replace('.', ',')
    
    rows += f'''<tr style="background-color: #f5f5f3; font-weight: bold;">
        <td style="font-size: 11px; color: #000; font-weight: bold;">TOTAL</td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;"></td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;">{fmt(total_sc)}</td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;">{fmt(total_iv)}</td>
        <td style="text-align: center; font-weight: bold; font-size: 11px; color: #000;"></td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;"></td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;"></td>
        <td style="text-align: center; font-size: 11px; color: #000; font-weight: bold;"></td>
    </tr>'''
    
    ps_section = f'<div class="ps"><strong>P.S. AI</strong> {ps_content}</div>' if ps_content else ""
    
    if is_empty:
        table_section = '<div style="text-align: center; padding: 40px 20px; font-size: 22px; font-weight: bold; color: #000;">Hier, nous n\'avons pas enregistré de volumes d\'Ocrevus facturés</div>'
    else:
        table_section = f"""<table>
                <thead>
                    <tr>
                        <th>Centres</th>
                        <th>Catégorie<br>de centres</th>
                        <th class="sc-header">Volume MTT<br>Ocrevus SC<br>d'hier</th>
                        <th class="iv-header">Volume MTT<br>Ocrevus IV<br>d'hier</th>
                        <th>Volume MTT<br>Ocrevus IV+SC<br>dans le mois</th>
                        <th>Nombre de<br>commandes<br>dans le mois<br>d'Ocrevus IV+SC</th>
                        <th>Date 1ère<br>facturation<br>Ocrevus SC</th>
                        <th>Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>"""
    
    ambition_section = f'''<div style="margin-top: 5px; font-size: 13px; font-style: italic; text-align: center; color: #777;">{ambition_text}</div>''' if ambition_text else ""
    
    chart1_legend = '<div style="margin-top: 5px; font-size: 13px; font-style: italic; text-align: center; color: #555;">Ambition : 70% des C1/C2 et 50% des C3 ont commandé Ocrevus SC<br>dans les 4 mois suivants le lancement soit 119 centres</div>'
    
    tracking_pixel = f'<img src="{TRACKER_URL}/pixel/{tracking_id}.png" width="1" height="1" style="display:none" alt="">' if tracking_id else ""

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        @font-face {{
            font-family: 'Roche Sans';
            src: url('https://github.com/LakovskyR/OCREVUS/blob/main/RocheSans-Regular.ttf?raw=true') format('truetype');
            font-style: normal;
            font-weight: normal;
        }}
        @font-face {{
            font-family: 'Roche Sans';
            src: url('https://github.com/LakovskyR/OCREVUS/blob/main/RocheSans-Italic.ttf?raw=true') format('truetype');
            font-style: italic;
            font-weight: normal;
        }}
        body {{ font-family: 'Roche Sans', 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background-color: #f5f5f3; }}
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
        .legend {{ display: flex; justify-content: center; margin: 20px 0; font-size: 16px; font-weight: bold; }}
        .legend-item {{ display: flex; align-items: center; margin: 0 30px; }}
        .legend-box {{ width: 30px; height: 20px; border-radius: 4px; margin-right: 15px; }}
        .separator {{ height: 2px; background: #e0e0e0; margin: 30px 0; }}
        .vertical-separator {{ width: 2px; background: #e0e0e0; }}
        .kpi-container {{ display: flex; justify-content: space-between; margin: 20px 0; gap: 20px; }}
        .kpi-card {{ flex: 1; text-align: center; }}
        .chart {{ text-align: center; margin: 20px 0; }}
        .signature {{ margin-top: 30px; font-size: 14px; line-height: 1.8; color: #000; }}
        .ps {{ margin-top: 20px; padding: 15px; background-color: #f0f5ff; border-left: 4px solid #646db1; font-size: 13px; font-style: italic; color: #000; }}
        .disclaimer {{ margin-top: 20px; padding: 10px; font-family: 'Roche Sans', 'Segoe UI', Arial, sans-serif; font-style: italic; font-size: 12px; color: #666; text-align: center; }}
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
                Veuillez trouver ci-dessous les points clés du jour :<br>
                - Les centres livrés en Ocrevus hier ;<br>
                - Un focus sur la performance d'Ocrevus SC ;<br>
                - Un point de situation sur le mois en cours.<br><br>
                N'hésitez pas à compléter ces informations avec celles du <a href="https://eu-west-1a.online.tableau.com/#/site/tabemeacloud/views/DashboardNeurologie/Ventesinternes?:iid=1">dashboard neuro</a> et <a href="https://customer.roche.com/customer-focus">CES</a> !<br><br>
                🌟 <strong>Les centres livrés en Ocrevus hier :</strong>
            </div>
            {table_section}
            
            <div class="section-title">🎯 Où en est-on au niveau national à date ?</div>
            
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
                <div class="kpi-card">
                    <img src="cid:kpi_chart" style="width: 100%; border-radius: 4px;">
                    {chart1_legend}
                </div>
                <div class="vertical-separator"></div>
                <div class="kpi-card">
                    <img src="cid:vol_chart" style="width: 100%; border-radius: 4px;">
                    {ambition_section}
                </div>
            </div>
            
            <div class="separator"></div>
            
            <div class="chart"><img src="cid:daily_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            
            <div class="separator"></div>
            
            <div class="section-title">🚀 Où en est-on sur les 12 derniers mois ?</div>
            <div class="chart"><img src="cid:monthly_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            
            <div class="signature">
                Merci à tous pour votre engagement autour d'Ocrevus SC ! Keep going 🚀<br><br>
                Bien à vous,<br>
                <strong>Nele et Diane-Laure</strong>
            </div>
            
            <div class="disclaimer">
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
        
        # Extract main data
        df_raw = fetch_tableau_data()
        df_rated_centers = fetch_tableau_view('rated_centers')
        
        # Load ambitions from local CSV
        csv_path = os.path.join(os.path.dirname(__file__), 'ambitions.csv')
        df_ambitions = pd.read_csv(csv_path)
        print(f"✓ Loaded ambitions from CSV: {csv_path}")
        
        # Transform
        df = unpivot_data(df_raw)
        final_table, query_date = calculate_metrics(df)
        
        # Charts (pass rated_centers for percentage calculation)
        vol_iv, vol_sc, total_centers = generate_charts(df, query_date, df_rated_centers)
        
        # Date
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%d/%m/%Y')
        
        # Generate ambition text (DYNAMIC from Tableau)
        ambition_text = generate_ambition_text(df_ambitions, yesterday)
        print(f"✓ Ambition text: {ambition_text}")
        
        # National metrics
        nat_iv = int(final_table["Volume MTT Ocrevus IV d'hier"].sum())
        nat_sc = int(final_table["Volume MTT Ocrevus SC d'hier"].sum())
        
        print("--- Sending Emails ---")
        
        if ACTIVE_RECIPIENT_GROUP == 'prod_sectorised':
            # Send sectorized emails for Promo
            promo_sectors = final_table['secteur_promo'].dropna().unique()
            for sector in sorted(promo_sectors):
                df_sec = final_table[final_table['secteur_promo'] == sector].copy()
                recipients = []
                for mail in df_sec['email_promo'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                if recipients:
                    sec_iv = int(df_sec["Volume MTT Ocrevus IV d'hier"].sum())
                    sec_sc = int(df_sec["Volume MTT Ocrevus SC d'hier"].sum())
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers, sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id, ambition_text)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Send sectorized emails for Medical
            medical_sectors = final_table['secteur_medical'].dropna().unique()
            for sector in sorted(medical_sectors):
                df_sec = final_table[final_table['secteur_medical'] == sector].copy()
                recipients = []
                for mail in df_sec['email_medical'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                if recipients:
                    sec_iv = int(df_sec["Volume MTT Ocrevus IV d'hier"].sum())
                    sec_sc = int(df_sec["Volume MTT Ocrevus SC d'hier"].sum())
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers, sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id, ambition_text)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Send sectorized emails for MA
            ma_sectors = final_table['secteur_ma'].dropna().unique()
            for sector in sorted(ma_sectors):
                df_sec = final_table[final_table['secteur_ma'] == sector].copy()
                recipients = []
                for mail in df_sec['email_ma'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                if recipients:
                    sec_iv = int(df_sec["Volume MTT Ocrevus IV d'hier"].sum())
                    sec_sc = int(df_sec["Volume MTT Ocrevus SC d'hier"].sum())
                    ps_content = get_ai_content(nat_iv, nat_sc, total_centers, sector_name=sector, sector_iv=sec_iv, sector_sc=sec_sc)
                    subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
                    tracking_id = generate_tracking_id(recipients[0], sector, date_str)
                    html = build_html_v4(df_sec, ps_content, tracking_id, ambition_text)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # Send national view to managers
            print("Sending National View to Managers...")
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers)
            subject_nat = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
            tracking_id = generate_tracking_id(RECIPIENT_GROUPS['prod_national_view'][0], 'NATIONAL', date_str)
            html_nat = build_html_v4(final_table, ps_content, tracking_id, ambition_text)
            send_email(RECIPIENT_GROUPS['prod_national_view'], subject_nat, html_nat)
        
        elif ACTIVE_RECIPIENT_GROUP == 'test_3':
            # Test mode: pick one active sector
            active_sectors = final_table[(final_table["Volume MTT Ocrevus IV d'hier"] > 0) | (final_table["Volume MTT Ocrevus SC d'hier"] > 0)]['secteur_promo'].unique()
            target_sector = active_sectors[0] if len(active_sectors) > 0 else final_table['secteur_promo'].unique()[0]
            df_sec = final_table[final_table['secteur_promo'] == target_sector].copy()
            sec_iv = int(df_sec["Volume MTT Ocrevus IV d'hier"].sum())
            sec_sc = int(df_sec["Volume MTT Ocrevus SC d'hier"].sum())
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers, sector_name=target_sector, sector_iv=sec_iv, sector_sc=sec_sc)
            subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
            tracking_id = generate_tracking_id(RECIPIENT_GROUPS['test_3'][0], target_sector, date_str)
            html = build_html_v4(df_sec, ps_content, tracking_id, ambition_text)
            send_email(RECIPIENT_GROUPS['test_3'], subject, html)
            
        else:
            # Standard modes (test_1, test_2, prod_csv, prod_national_view)
            if ACTIVE_RECIPIENT_GROUP == 'prod_csv':
                recipients = load_emails_from_csv(CSV_MAIL_LIST_URL)
            else:
                recipients = RECIPIENT_GROUPS.get(ACTIVE_RECIPIENT_GROUP, [SENDER_EMAIL])
            
            if not recipients:
                print(f"❌ Error: No recipients found for group {ACTIVE_RECIPIENT_GROUP}")
                sys.exit(1)
            
            subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str}"
            ps_content = get_ai_content(nat_iv, nat_sc, total_centers)
            tracking_id = generate_tracking_id(recipients[0], 'NATIONAL', date_str)
            html = build_html_v4(final_table, ps_content, tracking_id, ambition_text)
            send_email(recipients, subject, html)
        
        print("✅ Process Complete")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)