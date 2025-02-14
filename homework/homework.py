import pandas as pd
import os
import glob
import zipfile

def open_data():
    path = 'files/input'
    zip_files = glob.glob(f'{path}/*.zip')
    
    for zfile in zip_files:
        with zipfile.ZipFile(zfile, 'r') as handler:
            csv_files = [file for file in handler.namelist() if file.endswith('.csv')]
            for csv_file in csv_files:
                with handler.open(csv_file) as file:
                    df = pd.read_csv(file)
                    update_client(df.copy())
                    update_campaign(df.copy())
                    update_economics(df.copy())

def update_client(df):
    columnas = ['client_id','age','job','marital','education','credit_default','mortgage']
    df = df.loc[:, columnas]
    df['job'] = df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    df['education'] = df['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
    df['credit_default'] = df['credit_default'].map({'yes': 1}).fillna(0).astype(int)
    df['mortgage'] = df['mortgage'].map({'yes': 1}).fillna(0).astype(int)
    ruta = 'files/output/client.csv'
    os.makedirs('files/output', exist_ok=True)
    file_exists = os.path.exists(ruta)
    df.to_csv(ruta, mode='a', index=False, header=not file_exists)

def update_campaign(df):
    columnas = ['client_id','number_contacts','contact_duration','previous_campaign_contacts','previous_outcome',
                'campaign_outcome','month','day']
    df = df.loc[:, columnas]

    df['previous_outcome'] = df['previous_outcome'].map({'success': 1}).fillna(0).astype(int)
    df['campaign_outcome'] = df['campaign_outcome'].map({'yes': 1, 'no': 0}).fillna(0).astype(int)

    # Diccionario para convertir meses de texto a número
    month_to_number = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }

    # Convertimos el mes si está en formato texto
    df['month'] = df['month'].str.lower().map(month_to_number)

    # Asegurar que día y mes tengan dos dígitos
    df['month'] = df['month'].astype(str).str.zfill(2)
    df['day'] = df['day'].astype(str).str.zfill(2)

    # Crear columna de fecha correctamente formateada
    df['last_contact_date'] = "2022-" + df['month'] + "-" + df['day']

    # Eliminar columnas originales de mes y día
    df = df.drop(columns=['month', 'day']).copy()

    # Guardar archivo
    ruta = 'files/output/campaign.csv'
    os.makedirs('files/output', exist_ok=True)
    file_exists = os.path.exists(ruta)
    df.to_csv(ruta, mode='a', index=False, header=not file_exists)


def update_economics(df):
    columnas = ['client_id','cons_price_idx','euribor_three_months']
    df = df.loc[:, columnas]
    ruta = 'files/output/economics.csv'
    os.makedirs('files/output', exist_ok=True)
    file_exists = os.path.exists(ruta)
    df.to_csv(ruta, mode='a', index=False, header=not file_exists)

def clean_campaign_data():
    path = 'files/output'
    if os.path.exists(path):
        archivos = glob.glob(f'{path}/*.csv')
        for archivo in archivos:
            os.remove(archivo)
        os.rmdir(path)
    open_data()

if __name__ == "__main__":
    clean_campaign_data()
