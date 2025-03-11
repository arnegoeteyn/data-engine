import os
from dv_utils import  LogLevel, set_event, log
import requests 

# DATA_ENGINE_URL="http://localhost:3700"
DATA_ENGINE_URL="http://data-engine"

def event_processor(evt: dict):
  """
  Process an incoming event. The `evt` dict has at least the field `type`
  Exception raised by this function are handled by the default event listener and reported in the logs.
  """
  log("event_processor started", LogLevel.INFO)

  type = evt['type']
  c_id = netflix_id()
  if c_id == None:
    return
  
  if type == "status":

    endpoint = "{}/collaborators/{}".format(DATA_ENGINE_URL, c_id)
    res = requests.get(endpoint)

    log(str(res.content))
  
  elif type == "mount":
    endpoint = "{}/collaborators/{}".format(DATA_ENGINE_URL, c_id)
    res = requests.post(endpoint)
    print(res.content)
    log("requested mount")
    
  elif type == "query":
    endpoint = "{}/collaborators/{}/query".format(DATA_ENGINE_URL, c_id)
    headers = {"content-type": "application/json"}
    payload = { "select": "COUNT(*) as amount" }
    res = requests.request(method="get", url=endpoint, json=payload, headers=headers)
    log(str(res.content))
    
def netflix_id():
  return os.getenv("ID_NETFLIX_TITLES")
  

def dispatch_event_local(evt: dict):
  """
  Only for local use
  Sets the event that would be set by the listener in the cage
  """
  set_event(evt)
  event_processor(evt)

if __name__ == "__main__":
  """
  Only for local use
  Test events without a listener or redis queue set up
  """
  c_id = "6787f799e153203e00ae8c87"
  # evt = {
  #   "type": "status",
  # }
  # evt = {
  #   "type": "mount",
  # }
  evt = {
    "type": "query"
  }
  dispatch_event_local(evt)
