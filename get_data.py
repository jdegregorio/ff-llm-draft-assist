import requests
import pandas as pd
import os
import io

urls = [
    'https://github.com/dynastyprocess/data/raw/master/files/db_fpecr.parquet',
    'https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv',
    'https://raw.githubusercontent.com/dynastyprocess/data/master/files/values-picks.csv',
    'https://raw.githubusercontent.com/dynastyprocess/data/master/files/values-players.csv'
]

data_folder = './data/'
os.makedirs(data_folder, exist_ok=True)  # create the data folder if it doesn't exist

for url in urls:
    response = requests.get(url)
    file_name = os.path.basename(url).replace('-', '_')  # modify the filename
    file_path = os.path.join(data_folder, file_name)

    if file_name.endswith('.csv'):  # if the file is a CSV, convert it to Parquet
        df = pd.read_csv(io.BytesIO(response.content))
        df.to_parquet(file_path.replace('.csv', '.parquet'))
    else:  # otherwise, save it as is
        with open(file_path, 'wb') as f:
            f.write(response.content)
