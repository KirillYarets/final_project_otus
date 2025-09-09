from datetime import date, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine

from class_task_groups.configs import con_config

report_date_api = (date.today() - timedelta(days=1))


def nbrb_api():
    post_params = {"startdate": report_date_api, "enddate": report_date_api}
    currency = [431, 451, 456]

    try:
        for code in currency:
            api_nbrb_url = f'https://api.nbrb.by/exrates/rates/dynamics/{code}'
            # BASE_URL = f'https://api.nbrb.by/exrates/rates/{code}'

            r = requests.get(api_nbrb_url, post_params)
            my_dict = r.json()

            #print(my_dict)

            df = pd.DataFrame(my_dict)

            df.columns = ['cur_id', 'cur_date', 'cur_official_rate']

            #print(df)

            cnxn = create_engine(con_config['cnxn_eng'])

            df.to_sql('spr_nbrb_currency_rate', con=cnxn, schema='oplati_statistic',
                      if_exists='append', index=False)

    except Exception as base_er:
        print(base_er)
        quit()
