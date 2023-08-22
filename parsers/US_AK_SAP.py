from datetime import datetime, timezone, timedelta
from logging import Logger, getLogger
from typing import List, Optional, Union

from requests import Session

from electricitymap.contrib.lib.models.event_lists import ProductionBreakdownList, TotalConsumptionList
from electricitymap.contrib.lib.models.events import ProductionMix, TotalConsumption
from electricitymap.contrib.lib.types import ZoneKey
from parsers.lib.exceptions import ParserException
from parsers.lib.config import refetch_frequency

PARSER_NAME = "US_AK_SAP.py"
SOURCE = "seapahydro.org"
DATA_URL = "https://seapahydro.org/api/scada/index"

def get_value(data: dict, key: str, logger: Logger) -> float:
  try:
    return float(data[key]["text"])
  except ValueError:
    logger.error("Error casting to float: %s" % key)
    return None # return None if no value

@refetch_frequency(timedelta(hours=1))
def fetch_production(
  zone_key: ZoneKey,
  session: Session = Session(),
  target_datetime: Optional[datetime] = None,
  logger: Logger = getLogger(__name__),
) -> Union[List[dict], dict]:
  if target_datetime is not None:
    raise ParserException(
      PARSER_NAME, "This parser is not yet able to parse past dates", zone_key
    )

  res = session.get(DATA_URL)
  data = res.json()

  # 2 hydro plants owned by SEAPA
  total_hydro_power = get_value(data, "ss_mw", logger)  # `ss_mw` = `swl_mw` + `tyl_mw`

  production_mix = ProductionMix()
  if total_hydro_power == None:
    return None
  else:  
    production_mix.hydro = total_hydro_power

  production_list = ProductionBreakdownList(logger=logger)
  production_list.append(
    zoneKey=zone_key,
    datetime=datetime.now(timezone.utc),
    production=production_mix,
    source=SOURCE,
  )

  session.close()

  return production_list.to_list()

@refetch_frequency(timedelta(hours=1))
def fetch_consumption(
  zone_key: ZoneKey,
  session: Session = Session(),
  target_datetime: Optional[datetime] = None,
  logger: Logger = getLogger(__name__),
):
  if target_datetime is not None:
    raise ParserException(
      PARSER_NAME, "This parser is not yet able to parse past dates", zone_key
    )

  res = session.get(DATA_URL)
  data = res.json()

  # 3 cities receive power from SEAPA: Ketchikan, Petersburg, and Wrangell
  ktn_mw = get_value(data, "ktn_mw", logger) 
  ptg_mw = get_value(data, "ptg_mw", logger) 
  wrg_mw = get_value(data, "wrg_mw", logger)

  if any(x is None for x in [ktn_mw, ptg_mw, wrg_mw]):
    return None
  total_consumed_power = ktn_mw + ptg_mw + wrg_mw

  consumption_list = TotalConsumptionList(logger)
  consumption_list.append(
    zoneKey=zone_key,
    datetime=datetime.now(timezone.utc),
    consumption=total_consumed_power,
    source=SOURCE,
  )

  session.close()

  return consumption_list.to_list()

if __name__ == "__main__":
    """Main method, never used by the Electricity Maps backend, but handy for testing."""
    
    print(fetch_production(ZoneKey("US-AK-SAP")))
    print(fetch_consumption(ZoneKey("US-AK-SAP")))
