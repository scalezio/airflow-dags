from datetime import datetime


def get_datetime_now_iso():
    return datetime.utcnow().isoformat()


def parse_date(date_str):
    try:
        date_ = datetime.strptime(date_str, '%Y-%m-%d')
    except:
        try:
            date_ = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        except:
            try:
                date_ = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            except:
                try:
                    date_ = datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
                except:
                    date_ = datetime.strptime(date_str, '%d/%m/%Y')

    return datetime(date_.year, date_.month, date_.day, date_.hour, date_.minute, date_.second, date_.microsecond)
