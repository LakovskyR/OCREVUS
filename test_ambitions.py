import os
import io
import tableauserverclient as TSC
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOKEN_NAME = os.getenv('TABLEAU_TOKEN_NAME')
TOKEN_SECRET = os.getenv('TABLEAU_TOKEN_SECRET')
SITE_ID = os.getenv('TABLEAU_SITE_ID')
SERVER_URL = os.getenv('TABLEAU_SERVER_URL')
WORKBOOK_NAME = 'ocrevusMailData'

print(f"Connecting to: {SERVER_URL}")
print(f"Site ID: {SITE_ID}")
print(f"Workbook: {WORKBOOK_NAME}")

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
            if view.name == 'ambitions':
                target_view = view
                break
        if target_view: break
    
    if not target_view:
        print("ERROR: ambitions view not found!")
    else:
        print(f"Found view: {target_view.name}")
        server.views.populate_csv(target_view)
        csv_data = io.StringIO(b"".join(target_view.csv).decode("utf-8"))
        df = pd.read_csv(csv_data)
        
        print("\n=== AMBITIONS DATA ===")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nData types:\n{df.dtypes}")
        print(f"\nFirst 5 rows:\n{df.head()}")
        print(f"\nAll rows:\n{df}")
