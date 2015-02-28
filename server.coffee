csv = require 'csv-parse'
fs = require 'fs'
kd = require 'kdtree'
WebSocketServer = require('ws').Server

company_tree = new kd.KDTree(2)
service_tree = new kd.KDTree(2)

listen_port = 8374
num_businesses = 0

business_faults = []

# populate service tree
for i in [1 .. 1000]
  service_tree.insert(Math.random() * 360.0 - 180.0, Math.random() * 360.0 - 180.0, i)

company_query = (ws, json) ->
  center = json["center"]
  radius = json["radius"]
  ret = company_tree.nearestRange center[0], center[1], radius
  actualret = 
    type: json["type"]
    value: ret
  ws.send JSON.stringify actualret

service_query = (ws, json) ->
  center = json["center"]
  radius = json["radius"]
  ret = service_tree.nearestRange center[0], center[1], radius
  actualret = 
    type: json["type"]
    value: ret
  ws.send JSON.stringify actualret
  
closest_service_query = (ws, json) ->
  center = json["location"]
  ret = service_tree.nearest center[0], center[1]
  actualret = 
    type: json["type"]
    value: ret
  ws.send JSON.stringify actualret

fault_query = (ws, json) ->
  actualret = 
    type: json["type"]
    value: business_faults
  ws.send JSON.stringify actualret

resolve_fault = (ws, json) ->
  i = json["business_id"]
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
    handler ws, json

fault_gen = (wss) ->
  fault_gen_lam = ->
    setTimeout(fault_gen_lam, 5000)
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

parser.on 'readable', ->
  while record = parser.read()
    error "Malformed record of length #{record.length}" unless record.length is 3
    company_tree.insert parseFloat(record[1]), parseFloat(record[2]), record[0]
    num_businesses += 1

parser.on 'error', (err) -> error err.message
parser.on 'finish', start_server

parser.write fs.readFileSync('filtered-companies.csv', 'utf8')
parser.end()