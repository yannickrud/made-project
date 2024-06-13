from pipeline import run_pipeline
import os

def test_pipeline_output(tmpdir):
    sql_path = '/data/project.sqlite'
    
    # if database is present then delete it to check if it is created again by pipeline 
    if os.path.exists(os.getcwd() + sql_path):
        os.remove(os.getcwd() + sql_path)
    assert not os.path.exists(os.getcwd() + sql_path)


    base_url = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/weather_phenomena/historical/'
    run_pipeline(base_url)
    assert os.path.isfile(os.getcwd() + sql_path)

    

