csv = require 'csv-parse'
fs = require 'fs'
kd = require 'kdtree'
WebSocketServer = require('ws').Server

company_tree = new kd.KDTree(2)
service_tree = new kd.KDTree(2)

listen_port = 8374
num_businesses = 0

businesses = []
business_faults = []

# populate service tree
for i in [1 .. 1000]
  service_tree.insert(Math.random() * 360.0 - 180.0, Math.random() * 360.0 - 180.0, i)

company_query = (ws, json) -> businesses

service_query = (ws, json) ->
  services = service_tree.nearestRange 0.0, 0.0, 100000.0
  fixed = []
  for service in services
    fixed.push
      "position":
        "lat": service[0]
        "lng": service[1]
      "id": service[2]
  fixed

business_query = (ws, json) ->
  id = json["id"]
  businesses[id]

closest_service_query = (ws, json) ->
  center = json["location"]
  service_tree.nearest center[0], center[1]

fault_query = (ws, json) -> business_faults

resolve_fault = (ws, json) ->
  i = json["id"]
  business_faults = business_faults.filter (e) -> e isnt i
  

api_handlers =
  "company_query": company_query
  "service_query": service_query
  "closest_service_query": closest_service_query
  "fault_query": fault_query
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
    setTimeout(fault_gen_lam, 10000)
    return unless Math.random() > 0.7
    i = (Math.random() * num_businesses) | 0
    
    if business_faults.indexOf i < 0
      console.log "New fault at business #{i}"
      business_faults.push i
    
    wss.broadcast JSON.stringify
      type: "fault"
      value: i
  fault_gen_lam()
  

start_server = ->
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
    company_tree.insert parseFloat(record[1]), parseFloat(record[2]),
      "name": record[0]
      "id": num_businesses
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