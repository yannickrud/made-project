import requests
from lxml import html
from zipfile import ZipFile
import pandas as pd
from io import BytesIO, StringIO
from tqdm import tqdm
import re

from sqlalchemy import create_engine

def get_data_files(url: str) -> tuple[pd.DataFrame, list[bytes]]:
	zip_files = []
	r = requests.get(url)


	webpage = html.fromstring(r.content)
	links = webpage.xpath('//a/@href')

	for i, link in enumerate(tqdm(links)):
		if link.endswith('.txt'):
			meta_data = extract_meta_data_from_link(url, link)
			continue

		if not link.endswith('.zip'):
			continue

		response = requests.get(url + link)
		zip_files.append(response.content)

	return meta_data, zip_files



def extract_meta_data_from_link(url, link) -> pd.DataFrame:
	response = requests.get(url + link)
	lines = []
	for i, line in enumerate(StringIO(response.text)):
		split1 = line.split(maxsplit=6)
		if i == 0:
			split2 = split1[-1].split()
		elif i == 1:
			continue
		else:
			split2 = split1[6].strip()
			split2 = re.split('\s{2,}', split2)

		split1 = split1[:-1] + split2
		lines.append(split1)

	df = pd.DataFrame(lines)
	df.columns = df.iloc[0]
	df = df[1:]
	df.reset_index(drop=True, inplace=True)
	return df


def extract_data_from_zip(zip_files: list[bytes]) -> pd.DataFrame:
	"""
	Extracts data from a list of zip files and returns it as a pandas DataFrame.

	:param zip_files: A list of bytes representing the content of the zip files.
	:type zip_files: list[bytes]
	:return: A pandas DataFrame containing the extracted data.
	:rtype: pd.DataFrame
	"""
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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
	# convert start and end date of measurement to datetime
	time_related_columns = ['MESS_DATUM_BEGINN', 'MESS_DATUM_ENDE']
	df[time_related_columns] = df[time_related_columns].apply(pd.to_datetime, format="%Y%m%d")

	df.drop('eor', inplace=True)
	return df

def transform_meta_data(df):
	numeric_cols = ['Stations_id', 'Stationshoehe', 'geoBreite', 'geoLaenge']
	df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
	# von_datum, bis_datum
	time_related_columns = ['von_datum', 'bis_datum']
	df[time_related_columns] = df[time_related_columns].apply(pd.to_datetime, format="%Y%m%d")
	return df



if __name__ == '__main__':
	base_url = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/weather_phenomena/historical/'

	extract_meta_data_from_link(base_url, "wetter_jahreswerte_Beschreibung_Stationen.txt")
	# extract the data and convert to a pandas dataframe
	meta_data, zip_files = get_data_files(base_url)
	
	df = extract_data_from_zip(zip_files)

	meta_data = transform_meta_data(meta_data)
	df = transform_data(df)

	# create database
	engine = create_engine('sqlite:///data/project.sqlite', echo=False)
	# writes the meta data to table description and extracted data to table 'weather_phenomena'
	meta_data.to_sql('description', engine, if_exists='replace', index=False)
	df.to_sql('weather_phenomena', engine, if_exists='replace', index=False)


	exit(0)


