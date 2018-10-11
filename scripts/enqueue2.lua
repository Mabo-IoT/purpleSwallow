
--
--
--

-- 
local function unpack(input_type)
    local data
    if input_type == "json" then
     data = cjson.decode(ARGV[1])
    else
     data = cmsgpack.unpack(ARGV[1])
    end
    return data

end

-- 
local function pack(data, output_type)
    local msg 
    if output_type == "json" then    
        msg= cjson.encode(data)
    else
        msg= cmsgpack.pack(data)
    end 
    
    return msg
end

-- 
local function check(equipment, fields, timestamp, measurement, heartbeat_interval, datetime_str, host_ip)
    
    local thkey = "last:" .. equipment

    local old_fields = redis.call("HGET", thkey, "fields")
    
    local old_timestamp = redis.call("HGET", thkey, "timestamp")

    if old_fields == false or old_timestamp == false then
        -- init value here
        redis.call("HSET", thkey, "fields", fields)
        redis.call("HSET", thkey, "measurement", measurement)
        redis.call("HSET", thkey, "timestamp", timestamp)
        redis.call("HSET", thkey, "datetime", datetime_str)
        redis.call("HSET", thkey, "IP", host_ip)
        return true, true
    end

    local new_value = false
    local heartbeat_value = false
    
    if fields ~= old_fields then
        -- value changed
        new_value = true
        redis.call("HSET", thkey, "fields", fields)
        redis.call("HSET", thkey, "measurement", measurement)
        redis.call("HSET", thkey, "datetime", datetime_str)
        redis.call("HSET", thkey, "IP", host_ip)
    end
    
    if new_value == false and (tonumber(timestamp) - tonumber(old_timestamp) > heartbeat_interval ) then
        --  need a heartbeat
        heartbeat_value = true
        -- redis.call("HSET", thkey, "fields", fields)
        redis.call("HSET", thkey, "timestamp", timestamp)
        redis.call("HSET", thkey, "datetime", datetime_str)
        redis.call("HSET", thkey, "IP", host_ip)

    end
    
    return new_value, heartbeat_value
    
end

local function main()
    local heartbeat_interval = 3  -- in seconds

    local equipment = KEYS[1]

    local input_type = ARGV[2]
    local output_type = ARGV[3]
    
    local data = unpack(input_type)    
    local payload = data["data"]
    
    local host_ip = data["ziyan"]["hostIP"]
    
    local datetime_str = data["@timestamp"]
    local unit = data["time_unit"]
    
    local timestamp = payload["time"]
    local measurement = payload["measurement"]
    local fields = payload["fields"]
    local tags = payload["tags"]
    
    if unit == 'u' then
        heartbeat_interval = heartbeat_interval * 1000000
    elseif unit == 'm' then
        heartbeat_interval = heartbeat_interval * 1000
    end

    local new_value = nil
    local heartbeat_value = nil

    new_value, heartbeat_value = check(equipment, cjson.encode(fields), 
                                       timestamp, measurement, heartbeat_interval, datetime_str, host_ip)
    -- new_value = true
    if new_value == true then
        
        local data2 = {
            datatype = "data",
            data = data,
            unit = unit
        }
        -- local msg = cmsgpack.pack(data)
        local msg = pack(data2, dst)
        redis.call("LPUSH", "data_queue", msg) -- msg queue
        return 'OK'

    elseif heartbeat_value == true then
        
        -- fields2["heartbeat"] = 1
        
        local data2 = {
            datatype = "hb",
            data = data,
            unit = unit
        }
        -- local msg = cmsgpack.pack(data)   
        local msg = pack(data2, output_type)    
        redis.call("LPUSH", "data_queue", msg) -- msg queue
        return 'HEART_BEAT'
    else
        return 'SAME_VALUE'
    end
end

return main()
