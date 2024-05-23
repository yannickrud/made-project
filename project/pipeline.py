import requests
from lxml import html
from zipfile import ZipFile
import pandas as pd
from io import BytesIO
from tqdm import tqdm

from sqlalchemy import create_engine

def get_zip_files(url):
    zip_files = []
    r = requests.get(url)

    webpage = html.fromstring(r.content)
    links = webpage.xpath('//a/@href')

    for i, link in enumerate(tqdm(links)):
        if not link.endswith('.zip'):
            continue

        # if i == 10:
        #     break

        response = requests.get(url + link)
        zip_files.append(response.content)
    return zip_files


def extract_meta_data_from_zip(zip_files):
    pass


def extract_data_from_zip(zip_files):
    df_list = []
    for content in zip_files:
        with ZipFile(BytesIO(content)) as zip:
            names = zip.namelist()

            for name in names:
                if not name.startswith('produkt_'):
                    continue

                with zip.open(name) as f:
                    df = pd.read_csv(f, sep=';')
                    df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)
    return df


if __name__ == '__main__':
    base_url = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/monthly/weather_phenomena/historical/'

    zip_files = get_zip_files(base_url)
    df = extract_data_from_zip(zip_files)

    # create database
    engine = create_engine('sqlite:///../data/project.sqlite', echo=False)

    # writes the extracted data to database
    df.to_sql('weather_phenomena', engine, if_exists='replace', index=False)


    exit(0)


