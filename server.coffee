csv = require 'csv-parse'
fs = require 'fs'
kd = require 'kdtree'
WebSocketServer = require('ws').Server

company_tree = new kd.KDTree(2)
service_tree = new kd.KDTree(2)

listen_port = 8374
num_businesses = 0

businesses = []
all_services = []
business_faults = []

extract_center_and_range = (ob) ->
  if ob is undefined
    ret =
      "center":
        "lat": 0
        "lng": 0
      "range": 0
    return ret
  ne = ob["ne"]
  sw = ob["sw"]
  center =
    "lat": (ne["lat"] + sw["lat"]) / 2
    "lng": (ne["lng"] + sw["lng"]) / 2
  range = Math.sqrt Math.pow(ne["lat"] - sw["lat"], 2) + Math.pow(ne["lng"] - sw["lng"], 2)
  
  ret =
    "center": center
    "range": range
    
  ret

company_range = (ob) ->
  company_tree.nearestRange ob["center"]["lat"], ob["center"]["lng"], ob["range"]

service_range = (ob) ->
  service_tree.nearestRange ob["center"]["lat"], ob["center"]["lng"], ob["range"]

company_query = (ws, json) ->
  old_companies = company_range extract_center_and_range json["old"]
  companies = company_range extract_center_and_range json["new"]
  
  old_ids = old_companies.map (i) -> i[2]
  new_ids = companies.map (i) -> i[2]
  
  removed_ids = []
  added_ids = []
  
  for old_id in old_ids
    removed_ids.push(old_id) if new_ids.indexOf(old_id) < 0
  for new_id in new_ids
    added_ids.push(new_id) if old_ids.indexOf(new_id) < 0
  
  removed = removed_ids.map (i) ->
    bus = businesses[i]
    bus["fault"] = business_faults.indexOf(i) >= 0
    bus
  added = added_ids.map (i) ->
    bus = businesses[i]
    bus["fault"] = business_faults.indexOf(i) >= 0
    bus
  
  ret =
    "removed": removed
    "added": added
  ret

service_query = (ws, json) ->
  old_services = service_range extract_center_and_range json["old"]
  services = service_range extract_center_and_range json["new"]
  
  old_ids = old_services.map (i) -> i[2] - starting_service_id
  new_ids = services.map (i) -> i[2] - starting_service_id
  
  removed_ids = []
  added_ids = []
  
  for old_id in old_ids
    removed_ids.push(old_id) if new_ids.indexOf(old_id) < 0
  for new_id in new_ids
    added_ids.push(new_id) if old_ids.indexOf(new_id) < 0
  
  removed = removed_ids.map (i) -> all_services[i]
  added = added_ids.map (i) -> all_services[i]
  
  ret =
    "removed": removed
    "added": added
  ret
  

business_query = (ws, json) ->
  id = json["id"]
  businesses[id]

closest_service_query = (ws, json) ->
  center = json["location"]
  service_tree.nearest center[0], center[1]

resolve_fault = (ws, json) ->
  i = json["id"]
  business_faults = business_faults.filter (e) -> e isnt i
  

api_handlers =
  "company_query": company_query
  "service_query": service_query
  "closest_service_query": closest_service_query
  "resolve_fault": resolve_fault

handle_socket = (ws) ->
  ws.on 'message', (m) ->
    try
      json = JSON.parse m
    catch e
      console.log "Received malformed message '#{m}'"
      return
    handler = api_handlers[json["type"]]
    return unless handler?
    ret = handler ws, json["data"]
    actualret = 
      "type": json["type"]
      "value": ret
    ws.send JSON.stringify actualret

fault_gen = (wss) ->
  fault_gen_lam = ->
    setTimeout(fault_gen_lam, 100)
    
    return unless Math.random() > 0.9
    
    i = (Math.random() * num_businesses) | 0
    
    return unless business_faults.indexOf(i) < 0
    
    console.log "New fault at business #{i}"
    business_faults.push i
    wss.broadcast JSON.stringify
      type: "fault"
      value: i
  fault_gen_lam()
  
starting_service_id = 0

start_server = ->
  # populate service tree
  starting_service_id = num_businesses + 1
  service_id = starting_service_id
  for i in [0 .. 2000]
    business1 = businesses[(Math.random() * num_businesses) | 0]
    business2 = businesses[(company_tree.nearest business1["position"]["lat"], business1["position"]["lng"])[2]]
    pos = 
      "lat": (business1["position"]["lat"] + business2["position"]["lat"]) / 2
      "lng": (business1["position"]["lng"] + business2["position"]["lng"]) / 2
    latoff = (Math.random() - 0.5) / 5
    lngoff = (Math.random() - 0.5) / 5
    nlat = pos["lat"] + latoff
    nlng = pos["lng"] + lngoff
    service_tree.insert(nlat, nlng, service_id)
    all_services[i] =
      "position":
        "lat": nlat
        "lng": nlng
      "id": service_id
    service_id += 1
  wss = new WebSocketServer port: listen_port
  wss.broadcast = (d) -> wss.clients.forEach (c) -> c.send d
  wss.on 'connection', (ws) -> handle_socket ws
  console.log "Listening for connections on port #{listen_port}"
  fault_gen(wss)

error = (m) ->
  console.log m
  process.exit 1

# Read in company data
parser = csv delimiter: ','

business_id_it = 0
parser.on 'readable', ->
  while record = parser.read()
    error "Malformed record of length #{record.length}" unless record.length is 3
    company_tree.insert parseFloat(record[1]), parseFloat(record[2]), num_businesses
    businesses.push
      "position":
        "lat": parseFloat(record[1])
        "lng": parseFloat(record[2])
      "name": record[0]
      "id": num_businesses
    num_businesses += 1

parser.on 'error', (err) -> error err.message
parser.on 'finish', start_server

parser.write fs.readFileSync('filtered-companies.csv', 'utf8')
parser.end()