
import time
import json

import socket 

import msgpack
import redis


"""
# filebeat: 
{
  "@timestamp": "2018-06-13T06:19:45.511Z",
  "@metadata": {
    "beat": "",
    "type": "doc",
    "version": "6.2.4"
  },
  "beat": {
    "version": "6.2.4",
    "name": "MABO",
    "hostname": "MABO"
  },
  "source": "G:\\github\\maboio\\log\\GEM control arm - two axle-5.log",
  "offset": 2906,
  "message": "(09/28/2017 12:58:08) Warning [Stmgr] \"Ch 1 Force -- Lower Limit Tripped.\"",
  "tags": [
    "sim"
  ],
  "prospector": {
    "type": "log"
  }
}

"""

def test():
    #print("measure to redis")
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    lua = """
    local a = '{"a1":"b2"}'
    local c = cjson.decode(a)
    c["zz"] = "x"

    return cjson.encode(c)
    
    """
    multiply = r.register_script(lua)
    print(multiply())

def main():
    #print("measurement to redis")
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    ### r.set('foo', 'bar')
    ### rtn = r.get('foo')
    ### print(rtn)
    
    with open("enqueue2.lua","r") as fh:
        lua = fh.read()
    #print(lua)
    multiply = r.register_script(lua)
    
    measurement = 'mts_station'
    
    t = 122#time.time()
    
    fields = {"a":12.34, "c":int(t)}
    
    #fields = json.dumps(data)
    #fields = msgpack.packb(data)
    tags = {"eqpt_no":"MTS01"}
    #tags = json.dumps(tags)
    
    host_name = socket.gethostname() 
    #print(socket.gethostbyname(host_name))#
    host_ip = socket.gethostbyname(host_name)
    
    datetime_str = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    
    msg = {
        "@timestamp":datetime_str,
        "data":{
            "time":int(time.time()),
            "measurement":measurement,
            "fields":fields,
            "tags":tags
        },
        "time_unit":"s",
        "source":"test",
        "ziyan":{"version":"0.2","hostname":host_name, "hostIP":host_ip}
    }    
    msg_str = json.dumps(msg) 
    print(msg_str)    
    #data = msgpack.packb(data)    
    y = multiply(keys=['MTS01'], args=[msg_str, "json","json"])
    print(y)
   
if __name__ == "__main__"    :
    main()
    #test()