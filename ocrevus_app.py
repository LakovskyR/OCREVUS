# -*- coding: utf-8 -*-
"""
Ocrevus Automation Script v5.1
- Logic: "Monday Mode" -> Aggregates Friday+Saturday+Sunday for the "Daily" view.
- Logic: "Week Mode" -> Uses the latest available date (Max Date) for stability.
- Visual: Ambition text color reverted to Grey (#777).
- Visual: Added Total row to Sector Table (Bold).
- Visual: Added Center Counts to Cluster Bar Chart labels.
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

# Gmail
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')

# Perplexity AI
PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
USE_AI = os.getenv('USE_AI', '0') == '1'

# Tracker
TRACKER_URL = os.getenv('TRACKER_URL', 'https://ocrevus-tracker.onrender.com')

# Recipient Groups
RECIPIENT_GROUPS = {
    'test_1': ['romain.lakovsky@roche.com'],
    'test_2': ['romain.lakovsky@roche.com', 'stephane.dudouet@roche.com'],
    'test_3': ['romain.lakovsky@roche.com', 'stephane.dudouet@roche.com', 'pascale.nicolas.pn1@roche.com'],
    'prod_sectorised': [],  # Dynamic
    'prod_national_view': ['romain.lakovsky@roche.com'],
    'prod_csv': [] # Loaded from CSV
}

ACTIVE_RECIPIENT_GROUP = os.getenv('ACTIVE_RECIPIENT_GROUP', 'test_1')
CSV_MAIL_LIST_URL = 'mail_list.csv'
SENDER_EMAIL = GMAIL_USER

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def clean_number_french(val):
    """Parses numbers with French formatting (comma decimals, space thousands)."""
    if pd.isna(val) or val == '':
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).replace('\xa0', '').replace(' ', '')
    val_str = val_str.replace(',', '.')
    try:
        return float(val_str)
    except ValueError:
        return 0.0

def load_emails_from_csv(csv_path):
    """Loads emails from a local CSV file securely."""
    try:
        if not os.path.exists(csv_path):
            print(f"⚠️ CSV file not found: {csv_path}")
            return [SENDER_EMAIL]
        
        # Robust CSV reading
        df = pd.read_csv(csv_path, sep=None, engine='python') 
        if df.empty:
             print("⚠️ CSV file is empty.")
             return [SENDER_EMAIL]
             
        # Assume first column contains emails
        email_col = df.columns[0]
        emails = df[email_col].dropna().astype(str).str.strip().tolist()
        
        # Basic validation
        valid_emails = [e for e in emails if '@' in e and '.' in e]
        print(f"✅ Loaded {len(valid_emails)} emails from CSV.")
        return valid_emails
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return [SENDER_EMAIL]

def load_data():
    """Connects to Tableau and fetches the View data as CSV."""
    print("🔌 Connecting to Tableau Server...")
    tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, SITE_ID)
    server = TSC.Server(SERVER_URL, use_server_version=True)
    
    with server.auth.sign_in(tableau_auth):
        print("✅ Authenticated.")
        # Search for the view
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         'Détail données brutes'))
        all_views, pagination_item = server.views.get(req_option)
        
        if not all_views:
            raise ValueError("View 'Détail données brutes' not found on Tableau.")
        
        view_item = all_views[0]
        print(f"📥 Downloading data from view: {view_item.name} (ID: {view_item.id})")
        
        server.views.populate_csv(view_item)
        csv_data = io.StringIO(b"".join(view_item.csv).decode("utf-8"))
        df = pd.read_csv(csv_data)
        
        # Clean numeric columns
        cols_to_clean = ['Mesure de valeurs', 'Objectif', 'Prealable', 'Prioritaire']
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = df[col].apply(clean_number_french)
        
        # Parse Dates
        if 'Date de facturation' in df.columns:
            df['Date de facturation'] = pd.to_datetime(df['Date de facturation'], dayfirst=True, errors='coerce')
        
        return df

def generate_tracking_id(email, sector, date_str):
    """Generates a tracking ID."""
    raw = f"{email}-{sector}-{date_str}-{time.time()}"
    return hashlib.md5(raw.encode()).hexdigest()

def get_ai_content(nat_iv, nat_sc, total_centers, sector_name="NATIONAL", sector_iv=None, sector_sc=None):
    """Generates commentary using Perplexity AI or fallback text."""
    if not USE_AI:
        return "<!-- AI disabled -->"
    
    try:
        print("🤖 Querying Perplexity AI...")
        pplx = Perplexity(PERPLEXITY_API_KEY)
        
        context = f"""
        Date: {datetime.now().strftime('%d/%m/%Y')}
        Sujet: Rapport quotidien ventes Ocrevus (Roche).
        Données Nationales: IV={nat_iv}, SC={nat_sc}, Total Centres={total_centers}.
        """
        
        if sector_name != "NATIONAL":
            context += f"Données Secteur ({sector_name}): IV={sector_iv}, SC={sector_sc}."
            
        prompt = f"""
        Rédige un très court paragraphe (max 2 phrases) motivant et professionnel pour l'équipe commerciale 
        basé sur ces chiffres. Ton : dynamique, business. Langue : Français.
        Ne mentionne pas de chiffres précis, juste la tendance.
        {context}
        """
        
        answer = list(pplx.generate(prompt))[0]
        return f'<div style="margin-top:15px; padding:10px; background-color:#eef2f5; border-radius:5px; font-style:italic;">💡 " {answer["text"]} "</div>'
    except Exception as e:
        print(f"⚠️ AI Generation failed: {e}")
        return ""

def create_bar_chart_html(df_cumul, cluster_counts):
    """Creates a Bar Chart (Chart 1) with Plotly."""
    if df_cumul.empty:
        return ""
    
    # Add counts to Cluster labels
    df_plot = df_cumul.copy()
    df_plot['Cluster_Label'] = df_plot['Cluster'].apply(lambda x: f"{x} ({cluster_counts.get(x, 0)})")

    fig = px.bar(
        df_plot,
        x="Cluster_Label",
        y="Volumes",
        color="Type",
        title="<b>Volumes cumulés par Cluster</b>",
        color_discrete_map={"Ocrevus IV": "#001A72", "Ocrevus SC": "#36A9E1"},
        barmode="group",
        text_auto='.0f'
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        font=dict(family="Arial", size=12),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title=""
        ),
        xaxis_title="",
        yaxis_title="Volumes",
        bargap=0.15
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})

def create_line_chart_html(df_daily):
    """Creates a Line Chart (Chart 2) with Plotly."""
    if df_daily.empty:
        return ""
        
    df_daily_agg = df_daily.groupby(['Date de facturation', 'Type'])['Volumes'].sum().reset_index()
    
    fig = px.line(
        df_daily_agg,
        x="Date de facturation",
        y="Volumes",
        color="Type",
        title="<b>Évolution journalière (Mtd)</b>",
        color_discrete_map={"Ocrevus IV": "#001A72", "Ocrevus SC": "#36A9E1"},
        markers=True
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        font=dict(family="Arial", size=12),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title=""
        ),
        xaxis_title="",
        yaxis_title="Volumes"
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})

def create_donut_chart_html(df_sector_cumul):
    """Creates a Donut Chart (Chart 3) with Plotly."""
    if df_sector_cumul.empty:
        return ""
    
    total_vol = df_sector_cumul['Volumes'].sum()
    if total_vol == 0:
        return ""

    fig = px.pie(
        df_sector_cumul,
        values='Volumes',
        names='Type',
        title="<b>Répartition Mix (Cumul)</b>",
        color='Type',
        color_discrete_map={"Ocrevus IV": "#001A72", "Ocrevus SC": "#36A9E1"},
        hole=0.6
    )
    
    fig.update_traces(textinfo='percent+label')
    fig.update_layout(
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        annotations=[dict(text=f'Total<br>{int(total_vol)}', x=0.5, y=0.5, font_size=20, showarrow=False)]
    )
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})

def create_sector_table_html(df_sec, tracking_id):
    """Creates the HTML table for sector details with a TOTAL row."""
    if df_sec.empty:
        return "<p>Aucune donnée secteur.</p>"

    # Calculate Totals
    total_iv_hier = df_sec['Vol. IV (hier)'].sum()
    total_sc_hier = df_sec['Vol. SC (hier)'].sum()
    total_iv_cumul = df_sec['Vol. IV (cumul)'].sum()
    total_sc_cumul = df_sec['Vol. SC (cumul)'].sum()
    
    # Calculate Total Share %
    total_vol_cumul = total_iv_cumul + total_sc_cumul
    total_share = (total_sc_cumul / total_vol_cumul * 100) if total_vol_cumul > 0 else 0

    rows = ""
    for index, row in df_sec.iterrows():
        bg_color = "#f8f9fa" if index % 2 == 0 else "#ffffff"
        
        rows += f"""
        <tr style="background-color: {bg_color}; border-bottom: 1px solid #eee;">
            <td style="padding: 8px; font-weight: bold; color: #333;">{row['Secteur']}</td>
            <td style="padding: 8px; text-align: right;">{int(row['Vol. IV (hier)']) if row['Vol. IV (hier)']>0 else '-'}</td>
            <td style="padding: 8px; text-align: right; color: #36A9E1; font-weight:bold;">{int(row['Vol. SC (hier)']) if row['Vol. SC (hier)']>0 else '-'}</td>
            <td style="padding: 8px; text-align: right; border-left: 1px solid #eee;">{int(row['Vol. IV (cumul)'])}</td>
            <td style="padding: 8px; text-align: right;">{int(row['Vol. SC (cumul)'])}</td>
            <td style="padding: 8px; text-align: right; font-weight: bold;">{row['% SC']:.1f}%</td>
        </tr>
        """
    
    # Add Total Row
    total_row = f"""
    <tr style="background-color: #e6f0ff; border-top: 2px solid #001A72;">
        <td style="padding: 8px; font-weight: bold; color: #001A72;">TOTAL</td>
        <td style="padding: 8px; text-align: right; font-weight: bold;">{int(total_iv_hier)}</td>
        <td style="padding: 8px; text-align: right; color: #36A9E1; font-weight:bold;">{int(total_sc_hier)}</td>
        <td style="padding: 8px; text-align: right; border-left: 1px solid #ccc; font-weight: bold;">{int(total_iv_cumul)}</td>
        <td style="padding: 8px; text-align: right; font-weight: bold;">{int(total_sc_cumul)}</td>
        <td style="padding: 8px; text-align: right; font-weight: bold; color: #001A72;">{total_share:.1f}%</td>
    </tr>
    """

    table_html = f"""
    <table style="width: 100%; border-collapse: collapse; font-family: Arial, sans-serif; font-size: 13px;">
        <thead>
            <tr style="background-color: #001A72; color: white;">
                <th style="padding: 10px; text-align: left;">Secteur</th>
                <th style="padding: 10px; text-align: right;">IV (Hier)</th>
                <th style="padding: 10px; text-align: right;">SC (Hier)</th>
                <th style="padding: 10px; text-align: right; border-left: 1px solid #444;">IV (Mtd)</th>
                <th style="padding: 10px; text-align: right;">SC (Mtd)</th>
                <th style="padding: 10px; text-align: right;">% SC</th>
            </tr>
        </thead>
        <tbody>
            {rows}
            {total_row}
        </tbody>
    </table>
    """
    return table_html

def build_html_v4(df_sec, ps_content, tracking_id, ambition_text, charts_html):
    """Assembles the complete HTML email."""
    
    tracker_pixel = f'<img src="{TRACKER_URL}/track/{tracking_id}" width="1" height="1" style="display:none;" />'
    
    # Ambition section with GREY text (reverted)
    ambition_section = f'''<div style="margin-top: 5px; font-size: 13px; font-style: italic; text-align: center; color: #777;">{ambition_text}</div>''' if ambition_text else ""

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Arial', sans-serif; color: #333; margin: 0; padding: 0; background-color: #f4f4f4; }}
        .container {{ max-width: 800px; margin: 0 auto; background-color: #ffffff; }}
        .header {{ background-color: #001A72; padding: 20px; text-align: center; color: white; }}
        .content {{ padding: 20px; }}
        .kpi-box {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; margin-bottom: 20px; }}
        .footer {{ font-size: 11px; color: #888; text-align: center; padding: 20px; border-top: 1px solid #eee; }}
    </style>
    </head>
    <body>
    <div class="container">
        <div class="header">
            <h2 style="margin:0;">Rapport Quotidien Ocrevus</h2>
            <div style="font-size: 14px; margin-top: 5px;">Données arrêtées au {datetime.now().strftime('%d/%m/%Y')}</div>
            {ambition_section}
        </div>
        
        <div class="content">
            {ps_content}
            
            <h3 style="color: #001A72; border-bottom: 2px solid #001A72; padding-bottom: 5px; margin-top: 25px;">📊 Performance par Secteur</h3>
            {create_sector_table_html(df_sec, tracking_id)}
            
            <h3 style="color: #001A72; border-bottom: 2px solid #001A72; padding-bottom: 5px; margin-top: 30px;">📈 Dynamique Cluster & Mix</h3>
            
            <!-- Charts Layout -->
            <div style="display: flex; flex-wrap: wrap; gap: 20px; justify-content: center;">
                <div style="flex: 1; min-width: 300px;">
                    {charts_html['bar']}
                </div>
                <div style="flex: 1; min-width: 300px;">
                    {charts_html['donut']}
                </div>
            </div>
            
            <div style="margin-top: 20px;">
                {charts_html['line']}
            </div>

            <div style="margin-top: 30px; text-align: center;">
                <a href="{SERVER_URL}" style="background-color: #36A9E1; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold;">Accéder au Tableau Complet</a>
            </div>
        </div>

        <div class="footer">
            <p>Ce rapport est généré automatiquement. Merci de ne pas répondre directement à cet email.</p>
            {tracker_pixel}
        </div>
    </div>
    </body>
    </html>
    """
    return html

def send_email(recipients, subject, html_content):
    """Sends the email via SMTP."""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f"Ocrevus Bot <{GMAIL_USER}>"
    msg['To'] = ", ".join(recipients)

    part = MIMEText(html_content, 'html')
    msg.attach(part)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, recipients, msg.as_string())
        print(f"✅ Email sent to {len(recipients)} recipients.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("🚀 Starting Ocrevus Report Script v5.1...")
    
    # 1. Load Data
    try:
        df = load_data()
    except Exception as e:
        print(f"❌ Data load failed: {e}")
        sys.exit(1)

    # 2. Determine Date Mode (Monday/Weekend vs Standard)
    now = datetime.now()
    
    # If Today is MONDAY (weekday=0) -> "Daily" view is actually Weekend (Fri+Sat+Sun)
    if now.weekday() == 0:
        print("📆 Monday Mode detected. Aggregating Weekend data (Fri-Sun).")
        
        # Calculate target dates: Last Friday, Saturday, Sunday
        # Note: We rely on the date in the data, stripping time just in case
        sunday_dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        saturday_dt = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        friday_dt = (now - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        target_range = [friday_dt, saturday_dt, sunday_dt]
        
        # Filter DF for these 3 days
        # We assume df dates are already normalized to midnight by load_data's dayfirst=True
        df_day = df[df['Date de facturation'].isin(target_range)]
        
        # Reference date for "Month to Date" ending is Sunday
        report_ref_date = sunday_dt
        date_str_display = f"Weekend ({friday_dt.strftime('%d/%m')} - {sunday_dt.strftime('%d/%m')})"
        date_str_file = now.strftime('%d/%m/%Y') # Use Today for file/subject sorting if needed
        
    else:
        # Standard Mode: Use Max Available Date (usually Yesterday)
        # This fixes the issue where stale data caused an error or missed new data
        print("📆 Standard Mode. Using Max Available Date.")
        
        report_ref_date = df['Date de facturation'].max()
        df_day = df[df['Date de facturation'] == report_ref_date]
        
        date_str_display = report_ref_date.strftime('%d/%m/%Y')
        date_str_file = date_str_display

    if df_day.empty:
        print(f"⚠️ Warning: No daily data found for target period: {date_str_display}")
    
    # 4. Filter Cumulative Data (Current Month up to Report Date)
    start_date_month = report_ref_date.replace(day=1)
    df_month = df[(df['Date de facturation'] >= start_date_month) & (df['Date de facturation'] <= report_ref_date)]

    # 5. Process Data for Tables/Charts
    if 'Secteur' not in df.columns:
        df['Secteur'] = 'National' 
    
    # Aggregation (df_day now handles the sum of Fri+Sat+Sun automatically via groupby)
    pivot_day = df_day.groupby(['Secteur', 'Type'])['Volumes'].sum().unstack(fill_value=0)
    pivot_cumul = df_month.groupby(['Secteur', 'Type'])['Volumes'].sum().unstack(fill_value=0)
    
    # Ensure columns exist
    for col in ['Ocrevus IV', 'Ocrevus SC']:
        if col not in pivot_day.columns: pivot_day[col] = 0
        if col not in pivot_cumul.columns: pivot_cumul[col] = 0
        
    # Merge
    df_sec = pd.DataFrame({
        'Secteur': pivot_cumul.index,
        'Vol. IV (hier)': pivot_day['Ocrevus IV'][pivot_cumul.index].values,
        'Vol. SC (hier)': pivot_day['Ocrevus SC'][pivot_cumul.index].values,
        'Vol. IV (cumul)': pivot_cumul['Ocrevus IV'].values,
        'Vol. SC (cumul)': pivot_cumul['Ocrevus SC'].values
    })
    
    # Calculate %
    df_sec['Total Cumul'] = df_sec['Vol. IV (cumul)'] + df_sec['Vol. SC (cumul)']
    df_sec['% SC'] = (df_sec['Vol. SC (cumul)'] / df_sec['Total Cumul'] * 100).fillna(0)
    
    # Sort by Total Volume desc
    df_sec = df_sec.sort_values('Total Cumul', ascending=False)

    # 6. Generate Charts
    if 'Cluster' in df_month.columns:
        df_cluster_cumul = df_month.groupby(['Cluster', 'Type'])['Volumes'].sum().reset_index()
        cluster_counts = df_month.groupby('Cluster')['Nom du centre'].nunique().to_dict()
    else:
        df_cluster_cumul = pd.DataFrame()
        cluster_counts = {}

    bar_html = create_bar_chart_html(df_cluster_cumul, cluster_counts)
    line_html = create_line_chart_html(df_month)
    donut_html = create_donut_chart_html(df_cluster_cumul)
    
    charts = {'bar': bar_html, 'line': line_html, 'donut': donut_html}

    # 7. Generate & Send Email
    ambition_text = "ambition décembre : volumes Ocrevus IV : 2157 / volumes Ocrevus SC : 373 / Split SC/IV : 17%"
    
    if ACTIVE_RECIPIENT_GROUP == 'prod_csv':
        recipients = load_emails_from_csv(CSV_MAIL_LIST_URL)
    else:
        recipients = RECIPIENT_GROUPS.get(ACTIVE_RECIPIENT_GROUP, [SENDER_EMAIL])

    if not recipients:
        print("❌ No recipients found.")
        sys.exit(0)

    # AI Content
    nat_iv = df_sec['Vol. IV (cumul)'].sum()
    nat_sc = df_sec['Vol. SC (cumul)'].sum()
    total_centers = df_month['Nom du centre'].nunique()
    ps_content = get_ai_content(nat_iv, nat_sc, total_centers)

    # Build HTML
    tracking_id = generate_tracking_id("GLOBAL", "ALL", date_str_file)
    html_email = build_html_v4(df_sec, ps_content, tracking_id, ambition_text, charts)

    # Send
    subject = f"👉 Votre quotidienne Ocrevus SC/IV - {date_str_display}"
    send_email(recipients, subject, html_email)
    
    print("🏁 Script finished successfully.")

if __name__ == "__main__":
    main()