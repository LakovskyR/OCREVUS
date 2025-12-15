# -*- coding: utf-8 -*-
"""
Ocrevus Automation Script
Fetches data from Tableau, generates charts, sends sectorized emails.
Updated for overlapping networks (Promo/Medical/MA) with independent metrics.
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

# Third-party imports
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

# Load environment variables
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

# Active Group (Defaults to 'test_1' for safety)
ACTIVE_RECIPIENT_GROUP = os.getenv('ACTIVE_RECIPIENT_GROUP', 'test_1')

# Recipient Groups
RECIPIENT_GROUPS = {
    'test_1': [ "roman.lakovskiy@contractors.roche.com" ],
    'test_2': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "diane-laure.trouvet@roche.com"
    ],
    # People who ALWAYS get the National View (even in prod)
    'prod_national_view': [
        "roman.lakovskiy@contractors.roche.com",
        "amaury.coumau@roche.com",
        "nele.kokel@roche.com",
        "diane-laure.trouvet@roche.com"
    ]
}

# Styling
COLORS = { 'ocrevus_sc': '#ffc72a', 'ocrevus_iv': '#646db1', 'background': '#f5f5f3' }
FONT_FAMILY = 'Arial'
CHART_TITLE_SIZE = 18
CHART_TEXT_MAIN = 14
CHART_ANNOTATION = 15
CHART_TEXT_STANDARD = 13

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

def process_data(df_raw):
    print("--- Processing Data ---")
    cols = [c for c in df_raw.columns if c not in ['Measure Names', 'Measure Values']]
    df = df_raw.pivot(index=cols, columns='Measure Names', values='Measure Values').reset_index()
    df.columns.name = None
    
    # Rename
    df = df.rename(columns={
        'Day of Date Day': 'date_day', 'Center Cip': 'center_cip', 'Center Name': 'center_name',
        'Chainage Cip': 'chainage_cip', 'Chainage Name': 'chainage_name', 'Rating': 'category',
        'Secteur Promo': 'secteur_promo', 'Secteur Medical': 'secteur_medical', 'Secteur Ma': 'secteur_ma',
        'Email Promo': 'email_promo', 'Email Medical': 'email_medical', 'Email Ma': 'email_ma',
        'Volume Ocrevus Iv': 'volume_iv', 'Volume Ocrevus Sc': 'volume_sc'
    })
    
    # Date Parse
    month_map = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04', 'mai': '05', 'juin': '06',
        'juillet': '07', 'août': '08', 'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
        'janv.': '01', 'févr.': '02', 'avr.': '04', 'juil.': '07', 'sept.': '09', 'oct.': '10', 'nov.': '11', 'déc.': '12'
    }
    def parse_date(x):
        try:
            parts = x.strip().split()
            return pd.to_datetime(f"{parts[2]}-{month_map.get(parts[1].rstrip(',').lower(), '01')}-{parts[0].zfill(2)}")
        except: return pd.NaT

    df['date_day'] = df['date_day'].apply(parse_date)
    df['volume_iv'] = df['volume_iv'].fillna(0)
    df['volume_sc'] = df['volume_sc'].fillna(0)
    return df

def calculate_metrics(df):
    print("--- Calculating Metrics ---")
    today = datetime.now().date()
    yesterday = today - timedelta(days=3 if today.weekday() == 0 else 1)
    
    # Filter Yesterday
    if today.weekday() == 0:
        df_yesterday = df[(df['date_day'].dt.date >= yesterday) & (df['date_day'].dt.date < today)].copy()
    else:
        df_yesterday = df[df['date_day'].dt.date == yesterday].copy()
        
    df_table = df_yesterday.groupby(['chainage_cip', 'chainage_name']).agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_table.columns = ['chainage_cip', 'Chainage', 'Volume MTT Ocrevus IV de la veille', 'Volume MTT Ocrevus SC de la veille']
    
    # MTD
    current_month = today.replace(day=1)
    df_mtd = df[df['date_day'].dt.date >= current_month].copy()
    df_mtd_agg = df_mtd.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum', 'center_cip': 'count'}).reset_index()
    df_mtd_agg.columns = ['chainage_cip', 'volume_iv_mtd', 'volume_sc_mtd', 'nb_orders_mtd']
    
    # 4M Avg
    start_4m = current_month - timedelta(days=120)
    df_4m = df[(df['date_day'].dt.date >= start_4m) & (df['date_day'].dt.date < current_month)].copy()
    df_4m_agg = df_4m.groupby('chainage_cip').agg({'volume_iv': 'sum', 'volume_sc': 'sum'}).reset_index()
    df_4m_agg['avg_4m'] = (df_4m_agg['volume_iv'] + df_4m_agg['volume_sc']) / 4.0
    
    # Metadata
    df_first_sc = df[df['volume_sc'] > 0].groupby('chainage_cip')['date_day'].min().reset_index()
    df_first_sc.columns = ['chainage_cip', 'date_first_sc']
    
    cats = df.groupby('chainage_cip')['category'].first().reset_index()
    sector_info = df[['chainage_cip', 'secteur_promo', 'email_promo', 'secteur_medical', 'email_medical', 'secteur_ma', 'email_ma']].drop_duplicates('chainage_cip')
    
    # Merge
    final = df_table.merge(df_mtd_agg, on='chainage_cip', how='left') \
                    .merge(df_4m_agg, on='chainage_cip', how='left') \
                    .merge(df_first_sc, on='chainage_cip', how='left') \
                    .merge(cats, on='chainage_cip', how='left') \
                    .merge(sector_info, on='chainage_cip', how='left')
    
    # Fill & Calc
    final['Volume MTT Ocrevus IV+SC dans le mois'] = final['volume_iv_mtd'].fillna(0) + final['volume_sc_mtd'].fillna(0)
    final['Nombre de commandes dans le mois d\'Ocrevus IV+SC'] = final['nb_orders_mtd'].fillna(0).astype(int)
    final['Date 1ère commande Ocrevus SC'] = final['date_first_sc'].dt.strftime('%d/%m/%Y').fillna('')
    final['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois'] = final['avg_4m'].fillna(0).round(2)
    final['Catégorie de centres'] = final['category'].fillna('N/A')
    
    return final.fillna('')

# =============================================================================
# CHARTS
# =============================================================================

def generate_charts(df_full):
    print("--- Generating Charts ---")
    
    # KPI
    df_sc = df_full[df_full['volume_sc'] > 0][['chainage_cip', 'category']].drop_duplicates()
    df_kpi = df_sc.groupby('category').size().reset_index(name='count')
    df_kpi = pd.DataFrame({'category': ['C1','C2','C3','C4']}).merge(df_kpi, on='category', how='left').fillna(0)
    total_sc = int(df_kpi['count'].sum())
    
    fig_kpi = px.bar(df_kpi, x='category', y='count', text='count', color_discrete_sequence=[COLORS['ocrevus_sc']])
    fig_kpi.update_layout(template='plotly_white', height=450, width=600, font=dict(family=FONT_FAMILY, size=CHART_TEXT_MAIN),
                          title=dict(text='Centres initiateurs SC', font=dict(size=CHART_TITLE_SIZE)), 
                          xaxis=dict(title=None), yaxis=dict(title=None, showgrid=False, visible=False),
                          margin=dict(b=160, t=80)) # Increased bottom margin for text visibility
    fig_kpi.add_annotation(text=f'<b>{total_sc}</b>', x=0.95, y=1, xref="paper", yref="paper", showarrow=False, font=dict(size=36))
    fig_kpi.add_annotation(text="Ambition : 70% des C1/C2 et 50% des C3 ont commandé Ocrevus SC<br>dans les 4 mois suivants le lancement soit 119 centres", 
                           x=0.5, y=-0.28, xref="paper", yref="paper", showarrow=False, font=dict(size=CHART_ANNOTATION))
    fig_kpi.write_image('/tmp/kpi.png', scale=2)
    
    # PIE
    cur_month = datetime.now().replace(day=1).date()
    df_mtd = df_full[df_full['date_day'].dt.date >= cur_month]
    iv, sc = df_mtd['volume_iv'].sum(), df_mtd['volume_sc'].sum()
    
    fig_vol = go.Figure(data=[go.Pie(labels=['IV', 'SC'], values=[iv, sc], marker=dict(colors=[COLORS['ocrevus_iv'], COLORS['ocrevus_sc']]), 
                        textinfo='label+value+percent', textfont=dict(size=CHART_TEXT_MAIN))])
    # Legend right side to avoid overlap
    fig_vol.update_layout(template='plotly_white', height=450, width=600, title=dict(text='Volumes Ocrevus SC/IV - Mois en cours', font=dict(size=CHART_TITLE_SIZE)),
                          showlegend=True, legend=dict(x=1, y=0.5), margin=dict(l=20, r=100, t=80, b=50))
    fig_vol.write_image('/tmp/vol.png', scale=2)
    
    # DAILY (Last 5 Business Days)
    yesterday = datetime.now().date() - timedelta(days=1)
    
    # Find last 5 business days
    business_days = []
    curr = yesterday
    while len(business_days) < 5:
        if curr.weekday() < 5: # Mon-Fri
            business_days.append(curr)
        curr -= timedelta(days=1)
    business_days = sorted(business_days)
    
    df_d = df_full[df_full['date_day'].dt.date.isin(business_days)].groupby('date_day').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().sort_values('date_day')
    df_d['day'] = df_d['date_day'].dt.strftime('%a')
    
    fig_d = go.Figure()
    # Centered data labels inside
    fig_d.add_trace(go.Bar(x=df_d['day'], y=df_d['volume_iv'], name='IV', marker=dict(color=COLORS['ocrevus_iv']), 
                           text=df_d['volume_iv'].astype(int), textposition='inside', insidetextanchor='middle'))
    if df_d['volume_sc'].sum() > 0:
        fig_d.add_trace(go.Bar(x=df_d['day'], y=df_d['volume_sc'], name='SC', marker=dict(color=COLORS['ocrevus_sc']), 
                               text=df_d['volume_sc'].astype(int), textposition='inside', insidetextanchor='middle'))
    
    fig_d.update_layout(barmode='stack', template='plotly_white', height=400, width=900, title=dict(text='Daily Ocre SC IV', font=dict(size=CHART_TITLE_SIZE)), yaxis=dict(visible=False), showlegend=False)
    fig_d.write_image('/tmp/daily.png', scale=2)
    
    # MONTHLY
    df_full['m'] = df_full['date_day'].dt.to_period('M')
    df_m = df_full.groupby('m').agg({'volume_iv':'sum','volume_sc':'sum'}).reset_index().tail(12)
    df_m['lbl'] = df_m['m'].dt.strftime('%m/%y')
    
    fig_m = go.Figure()
    # Explicit names and colors for correct legend
    fig_m.add_trace(go.Bar(x=df_m['lbl'], y=df_m['volume_iv'], name='IV', marker=dict(color=COLORS['ocrevus_iv']), text=[f'{v/1000:.2f}K' for v in df_m['volume_iv']], textposition='outside'))
    fig_m.add_trace(go.Bar(x=df_m['lbl'], y=df_m['volume_sc'], name='SC', marker=dict(color=COLORS['ocrevus_sc']), text=[f'{v/1000:.2f}K' if v>0 else '' for v in df_m['volume_sc']], textposition='outside'))
    fig_m.update_layout(barmode='stack', template='plotly_white', height=400, width=900, title=dict(text='Monthly Ocre SC IV', font=dict(size=CHART_TITLE_SIZE)), yaxis=dict(title='Total Qté UE Mois'), showlegend=True)
    fig_m.write_image('/tmp/monthly.png', scale=2)
    
    return int(iv), int(sc)

# =============================================================================
# EMAIL GENERATION
# =============================================================================

def get_ai_content(iv, sc, total_centers):
    try:
        client = Perplexity(api_key=PPLX_API_KEY)
        today = datetime.now().strftime('%d/%m/%Y')
        prompt = f"""Contexte Ocrevus (SEP): IV existant, SC lancé 12/12/2024.
Situation {today}: IV={iv}, SC={sc}, Centres SC={total_centers}.
En 2-3 phrases courtes en français (ton optimiste):
1. Rassurer sur le rythme global.
2. Mentionner l'attente des premières commandes SC.
IMPORTANT: Ne PAS inclure de références ou de citations comme [1], [2], etc. dans la réponse."""
        
        resp = client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model="sonar")
        content = resp.choices[0].message.content
        # Double clean just in case
        for i in range(1, 10):
            content = content.replace(f'[{i}]', '')
        return content.replace('**','').replace('*','')
    except:
        return "Le rythme global est bon. Nous attendons avec confiance les premières commandes SC."

def build_html_v3(table_df, ps_content):
    """
    Restored exact HTML structure from V3
    """
    # Sort by volume desc
    df_sorted = table_df.sort_values('Volume MTT Ocrevus IV+SC dans le mois', ascending=False)
    
    rows = ""
    for _, row in df_sorted.iterrows():
        rows += f"""
        <tr>
            <td style="font-size: 11px; color: #000;">{row['Chainage']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Catégorie de centres']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus SC de la veille']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus IV de la veille']}</td>
            <td style="text-align: center; font-weight: bold; font-size: 11px; color: #000;">{row['Volume MTT Ocrevus IV+SC dans le mois']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row["Nombre de commandes dans le mois d'Ocrevus IV+SC"]}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Date 1ère commande Ocrevus SC']}</td>
            <td style="text-align: center; font-size: 11px; color: #000;">{row['Moyenne des Volumes MTT Ocrevus IV+SC des 4 derniers mois']}</td>
        </tr>"""

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
        th {{ background-color: #646db1; color: white; padding: 12px 8px; text-align: center; font-weight: bold; border: 1px solid #5560a0; font-size: 11px; line-height: 1.3; min-width: 80px; }}
        td {{ padding: 10px 8px; border: 1px solid #e0e0e0; color: #000; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f0f0f0; }}
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
                Veuillez trouver ci-après les centres qui ont été livrés de l'Ocrevus la veille, avec un highlight sur Ocrevus SC, puis un état des lieux de où est ce qu'on en est dans le mois.<br><br>
                N'hésitez pas à compléter ces informations avec celles présentes dans le <a href="https://emea.thoughtspot.roche.com/#/insights/pinboard/0ae0ed50-7887-43a4-b617-33c7694126ee">dashboard neuro</a> et <a href="https://emea.thoughtspot.roche.com">CES</a> !<br><br>
                🌟 <strong>Les centres qui ont reçu de l'Ocrevus la veille :</strong>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Chainage</th>
                        <th>Catégorie<br>de centres</th>
                        <th>Volume MTT<br>Ocrevus SC<br>de la veille</th>
                        <th>Volume MTT<br>Ocrevus IV<br>de la veille</th>
                        <th>Volume MTT<br>Ocrevus IV+SC<br>dans le mois</th>
                        <th>Nombre de<br>commandes<br>dans le mois<br>d'Ocrevus IV+SC</th>
                        <th>Date 1ère<br>commande<br>Ocrevus SC</th>
                        <th>CM4</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <div class="section-title">🎯 Où ce qu'on en est au niveau national, à date ?</div>
            <div class="kpi-container">
                <div class="kpi-card"><img src="cid:kpi_chart" style="width: 100%; border-radius: 4px;"></div>
                <div class="kpi-card"><img src="cid:vol_chart" style="width: 100%; border-radius: 4px;"></div>
            </div>
            <div class="chart"><img src="cid:daily_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            <div class="section-title">🚀 Et où ce qu'on en est sur les 12 derniers mois ?</div>
            <div class="chart"><img src="cid:monthly_chart" style="width: 100%; max-width: 900px; border-radius: 4px;"></div>
            <div class="signature">
                Merci à tous pour l'engagement que vous avez autour d'Ocrevus SC ! Keep going, c'est collectivement qu'on y arrivera 🚀<br><br>
                Bien à vous,<br>
                <strong>Nele et Diane-Laure</strong>
            </div>
            <div class="ps"><strong>P.S. AI</strong> {ps_content}</div>
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
        print(f"✅ Sent to {recipients}")
    except Exception as e:
        print(f"❌ Error sending to {recipients}: {e}")

# =============================================================================
# MAIN LOGIC
# =============================================================================

if __name__ == "__main__":
    try:
        # 1. Fetch & Process
        df_raw = fetch_tableau_data()
        df = process_data(df_raw)
        final_table = calculate_metrics(df)
        
        # 2. Assets (National context)
        nat_iv, nat_sc = generate_charts(df)
        total_centers_sc = df[df['volume_sc'] > 0]['chainage_cip'].nunique()
        ps_content = get_ai_content(nat_iv, nat_sc, total_centers_sc)
        
        # 3. Sending Logic
        date_str = datetime.now().strftime('%d/%m/%Y')
        
        if ACTIVE_RECIPIENT_GROUP == 'prod_sectorised':
            # --- LOOP 1: PROMO SECTORS ---
            promo_sectors = final_table['secteur_promo'].dropna().unique()
            for sector in sorted(promo_sectors):
                df_sec = final_table[final_table['secteur_promo'] == sector].copy()
                
                # Get unique PROMO emails for this sector
                recipients = []
                for mail in df_sec['email_promo'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    # Metrics for subject line (specific to this sector view)
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory: IV={sec_iv}, SC={sec_sc}"
                    
                    html = build_html_v3(df_sec, ps_content)
                    send_email(list(set(recipients)), subject, html) # Use set to dedup within same sector
                    time.sleep(1)

            # --- LOOP 2: MEDICAL SECTORS ---
            med_sectors = final_table['secteur_medical'].dropna().unique()
            for sector in sorted(med_sectors):
                df_sec = final_table[final_table['secteur_medical'] == sector].copy()
                
                recipients = []
                for mail in df_sec['email_medical'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory: IV={sec_iv}, SC={sec_sc}"
                    
                    html = build_html_v3(df_sec, ps_content)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)

            # --- LOOP 3: MA SECTORS ---
            ma_sectors = final_table['secteur_ma'].dropna().unique()
            for sector in sorted(ma_sectors):
                df_sec = final_table[final_table['secteur_ma'] == sector].copy()
                
                recipients = []
                for mail in df_sec['email_ma'].dropna().unique():
                    if '@' in str(mail): recipients.append(str(mail).strip())
                
                if recipients:
                    sec_iv = int(df_sec['Volume MTT Ocrevus IV de la veille'].sum())
                    sec_sc = int(df_sec['Volume MTT Ocrevus SC de la veille'].sum())
                    subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}. Territory: IV={sec_iv}, SC={sec_sc}"
                    
                    html = build_html_v3(df_sec, ps_content)
                    send_email(list(set(recipients)), subject, html)
                    time.sleep(1)
            
            # --- FINAL: Send National View to Global Team ---
            print("Sending National View to Global Team...")
            global_recipients = RECIPIENT_GROUPS['prod_national_view']
            subject_nat = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}"
            html_nat = build_html_v3(final_table, ps_content)
            send_email(global_recipients, subject_nat, html_nat)
            
        else:
            # Test Mode (Manual Run)
            recipients = RECIPIENT_GROUPS.get(ACTIVE_RECIPIENT_GROUP, [SENDER_EMAIL])
            subject = f"OCREVUS {date_str}. National: IV={nat_iv}, SC={nat_sc}"
            html = build_html_v3(final_table, ps_content)
            send_email(recipients, subject, html)
            
        print("✅ Execution Complete")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)