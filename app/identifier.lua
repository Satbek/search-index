local M = {}

local digest = require('digest')
local msgpack = require('msgpack')
M.cmp_data = {}

local Identifier = {}

function Identifier:new(data)
    local obj = {}
    obj.hash = digest.md5_hex(msgpack.encode(data))
    obj.data = data
    return obj
end

function M.phone_number(phone_number)
    local data = {'phone_number', phone_number}
    return Identifier:new(data)
end

function M.cmp_data.phone_number(data_one, data_two)
    return #data_one == 2 and #data_two == 2 and (data_one[1] == data_two[1]) and (data_one[2] == data_two[2] ~= nil)
end

function M.email(email)
    local data = {'email', email}
    return Identifier:new(data)
end

function M.cmp_data.email(data_one, data_two)
    return #data_one == 2 and #data_two == 2 and (data_one[1] == data_two[1]) and (data_one[2] == data_two[2] ~= nil)
end

function M.passport_num(passport_num)
    local data = {'passport_num', passport_num}
    return Identifier:new(data)
end

function M.cmp_data.passport_num(data_one, data_two)
    return #data_one == 2 and #data_two == 2 and (data_one[1] == data_two[1]) and (data_one[2] == data_two[2] ~= nil)
end

function M.geoposition(longitude, latitude)
    local data = {'geoposition', longitude, latitude}
    return Identifier:new(data)
end

function M.cmp_data.geoposition(data_one, data_two)
    if #data_one ~= 3 then
        return false
    end
    if #data_two ~= 3 then
        return false
    end

    for i = 1, 3 do
        if data_one[i] ~= data_two[i] then
            return false
        end
    end
    return true
end

return M
